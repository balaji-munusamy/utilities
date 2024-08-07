import re

class TextUtils():

    @staticmethod
    def preprocess_text(text: str) -> str:
        """
        Preprocess the text.

        Parameters:
        - text (str): Text to be preprocessed.

        Returns:
        - preprocessed_text (str): Preprocessed text.
        """
        text = text.casefold()
        tokens = re.findall(r'\b\w+\b', text)
        tokens = [token for token in tokens if token.isalnum()]
        processed_text = ' '.join(tokens)
        return processed_text