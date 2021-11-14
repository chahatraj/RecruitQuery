from . import definition


class RqTokenizer:
    """Tokenizer for RecruitQuery."""

    @staticmethod
    def Tokenize(sql):
        return [token.lower() if token.lower() in definition.COMMAND_TOKENS else token for token in sql.split(" ")]
