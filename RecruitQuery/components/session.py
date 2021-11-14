from . import table as tb
from . import table_fetcher
from .expression import RqExpression
from .grouping import RqGrouping
from .ordering import RqOrdering
from .ordering import RqTableOrdering


class RqSession:
    """A class for RecruitQuery sessions."""

    def __init__(self, github, field_exprs, source=None, condition=None, groups=None, having=None, orders=None, limit=None):
        self._field_exprs = field_exprs
        self._source = source
        self._condition = condition
        self._groups = groups
        self._having = having
        self._orders = orders
        self._limit = limit

        rel_keys = RqExpression.ExtractTokensFromExpressions(self._field_exprs)
        if self._condition:
            rel_keys += RqExpression.ExtractTokensFromExpressions([self._condition])
        if self._groups:
            rel_keys += RqExpression.ExtractTokensFromExpressions(self._groups)
        if self._having:
            rel_keys += RqExpression.ExtractTokensFromExpressions([self._having])
        if self._orders:
            rel_keys += RqExpression.ExtractTokensFromExpressions(self._orders[0])
        rel_keys = list(set(rel_keys))

        # if there's '*' in select, where, group by, having, orders argument then the rel_keys is reduced to just '*'
        if u"*" in rel_keys:
            rel_keys = [u"*"]
        self._fetcher = table_fetcher.RqTableFetcher(github, rel_keys)

    def _GetEmptyTable(self):
        table = tb.RqTable()
        table.SetFields(self._field_exprs)
        return table

    def Execute(self):
        # source is either a label (eg. "google.issues") or a RqSession
        if self._source:
            source_table = self._source.Execute() if isinstance(self._source, RqSession) else self._fetcher.Fetch(self._source)
            if not source_table[:]:
                return self._GetEmptyTable()
            else:
                if u"*" in self._field_exprs:
                    self._field_exprs = source_table.GetFields()
        else:
            source_table = tb.RqTable()
            source_table.SetFields([u"Dummy Field"])
            source_table.Append([u"Dummy Value"])

        # evaluate where clause
        if self._condition:
            filtered_table = tb.RqTable()
            filtered_table.SetFields(source_table.GetFields())
            meets = RqExpression.EvaluateExpression(source_table, self._condition)
            for i, row in enumerate(source_table):
                if meets[i]:
                    filtered_table.Append(row)
        else:
            filtered_table = source_table
        if not filtered_table[:]:
            return self._GetEmptyTable()
        
        # evaluate all necessary expressions
        # in reversed order because we process from the rightmost item first
        select_tokens = RqExpression.ExtractTokensFromExpressions(self._field_exprs[:]) 
        eval_exprs = select_tokens
        if self._orders:
            order_tokens = RqExpression.ExtractTokensFromExpressions(self._orders[0])
            eval_exprs += order_tokens
        if self._having:
            having_tokens = RqExpression.ExtractTokensFromExpressions([self._having])
            eval_exprs += having_tokens
        if self._groups:
            eval_exprs += self._groups
        
        res_table = RqExpression.EvaluateExpressions(filtered_table, eval_exprs)

        # group by
        if self._groups:
            res_tables = RqGrouping.GenerateGroups(res_table, self._groups)
        else:
            res_tables = [res_table]

        # having
        if self._having:
            filtered_tables = []
            for table in res_tables:
                if all(RqExpression.EvaluateExpression(table, self._having)):
                    filtered_tables.append(table.SliceCol(0, len(table.GetFields()) - len(having_tokens)))
            res_tables = filtered_tables

        # order by
        if self._orders:
            for table in res_tables:
                table.Copy(table.SliceCol(0, len(table.GetFields()) - len(order_tokens)).Chain(RqExpression.EvaluateExpressions(table, self._orders[0])))
                ordering = RqOrdering(table, self._orders[1])
                table.Copy(ordering.Sort(keep_order_fields=True))
            ordering = RqTableOrdering(res_tables, self._orders[1])
            res_tables = ordering.Sort()

        # process select
        for table in res_tables:
            table.Copy(RqExpression.EvaluateExpressions(table, self._field_exprs))

        # check if all tokens in expressions are contained in aggregate functions
        check_exprs = [expr for expr in self._field_exprs if expr not in self._groups] if self._groups else self._field_exprs
        if RqExpression.IsAllTokensInAggregate(check_exprs):
            for table in res_tables:
                table = table.SetTable([table[0]])

        merged_table = tb.RqTable()
        merged_table.SetFields(res_tables[0].GetFields())
        for table in res_tables:
            for row in table:
                merged_table.Append(row)

        # process limit
        if self._limit:
            merged_table.SetTable(merged_table[:self._limit])

        return merged_table
