import pandas as pandas
from matplotlib import pyplot as pyplot
import seaborn as seaborn

class Plot:
    """
    Class to handle plot
    """

    def __init__(self, argv_width=15, argv_height=10):
        """
        Size is in inches
        """
        self._figure = pyplot
        self._width = argv_width
        self._height = argv_height
        self._figure.figure(
            figsize=(self._width, self._height)
            )

    def reset(self):
        """
        Reset the figure
        """
        self._figure = pyplot
        self._figure.figure(
            figsize=(self._width, self._height)
            )

    def plot_dist(self, argv_df, argv_x="", argv_show=True):
        """
        This is similar to a histogram
        Note this would be replaced with displot in a new version which isn't as nice
        """
        # plot the distribution
        seaborn.distplot(argv_df[argv_x])
        # to show it
        if argv_show:
            self._figure.show()

    def plot_barchart(self, argv_df, argv_x="", argv_y="", argv_hue="", argv_show=True):
        """
        Bar chart
        """
        # plot the boxplot
        # if no grouping (hue)
        if argv_hue == "":
            seaborn.boxplot(
                y=argv_df[argv_y],
                x=argv_df[argv_x]
                )
        else:
            seaborn.boxplot(
                y=argv_df[argv_y],
                x=argv_df[argv_x],
                hue=argv_df[argv_hue]
                )
        # to show it or not
        if argv_show:
            self._figure.show()

    def plot_scatter(self, argv_df, argv_x="", argv_y="", argv_hue="", argv_jitter_x=False, argv_jitter_y=False, argv_show=True):
        """
        Scatter plot
        """
        # plot the scatter
        seaborn.lmplot(
            data=argv_df,
            y=argv_y,
            x=argv_x,
            hue=argv_hue,
            x_jitter=argv_jitter_x,
            y_jitter=argv_jitter_y
            )
        # to show it or not
        if argv_show:
            self._figure.show()

    def plot_cat(self, argv_df, argv_x="", argv_y="", argv_hue="", argv_kind="bar", argv_show=True):
        """
        This is similar to a barchart
        """
        # if no y, we will use the frequency (count)
        if argv_kind == "count":
            seaborn.catplot(
                data=argv_df,
                y=argv_y,
                hue=argv_hue,
                kind=argv_kind
                )
        # plot with a y
        else:
            seaborn.catplot(
                data=argv_df,
                x=argv_x,
                y=argv_y,
                hue=argv_hue,
                kind=argv_kind
                )
        # to show it or not
        if argv_show:
            self._figure.show()