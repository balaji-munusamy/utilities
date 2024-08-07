from typing import List

import seaborn as sns
import matplotlib.pylab as plt
import pandas as pd

class VisualUtils():

    @staticmethod
    def draw_line_chart_from_csv(file_path: str, x_field: str, y_fields: List[str]) -> None:
        df = pd.read_csv(file_path)
        for field in y_fields:
            sns.lineplot(df, x=df[x_field], y=df[field], label=field, markers=True)
            sns.scatterplot(df, x=df[x_field], y=df[field])
        plt.xticks([])
        plt.xlabel("Pipeline Runs")
        plt.ylabel("Score")
        plt.title("Chat Evaluation")
        plt.legend()
        plt.savefig(file_path.replace(".csv", ".png"))



