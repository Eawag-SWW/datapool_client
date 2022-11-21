import pandas as pd
import cufflinks as _cf
import plotly as _plotly
from matplotlib import pyplot as _plt

from datapool_client.api.api import DataPool
from datapool_client.core.formatting import reshape
from datapool_client.core.plots import generate_meta_plot
from datapool_client.core.utilities import (
    determine_additional_meta_info_columns_of_meta_data_history,
)

_cf.go_offline()


class Plot:
    def __init__(self, **kwargs):
        self.__dp = DataPool(**kwargs)

    def plot_signal(
        self,
        source_name,
        variable_name=None,
        start="1900-01-01 00:00:00",
        end=None,
        plot_dynamic=True,
        auto_open=True,
        inline=False,
        filename="plot.html",
        show_query=False,
        **kw_plot_args,
    ):
        """
        Parameters
        ----------
        source_name:          str, specifying the name of the source instance
        variable_name:        str | list, of variable name(s)
        start:                str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        end:                  str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        plot_dynamic:         bool, specifying whether to plot dynamically or statically
        auto_open:            bool, open plot in browser automatically if inline if False
        inline:               bool, return the figure so you can open it in your jupyter notebook
        filename:             str, filename of plot.html if inline is False
        show_query:           bool, specifying whether to print the query
        **kw_plot_args:       key word arguments that will be passed down to plot function. If plot_dynamic=True
                              keyword arguments will be passed to cufflinks (.iplot method) otherwise the arguments
                              will be passed matplotlib/pandas' (.plot method)

        Return
        ------
        signal_data: pandas.DataFrame, fig: plotly.Figure if inline=True


        Example
        -------

        # plotting everything from a source
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg")

        # plotting one variable of a source
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg","battery voltage")

        # plotting between time
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg","battery voltage","2019-01-01","2019-01-07")

        #plotting multiple variables of a source
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg",["SNR","battery voltage"])


        --- with **kw_plot_args ---

        ### passed to pandas/matplotlib ###

        # static plot splitted into subplots, specifying the figuresize
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg",plot_dynamic=False, subplots = True, figsize=(16,16))

        ### passed to cufflinks ###

        # dynamic plot with rangeslider
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg", rangeslider=True)

        # dynamic plot each signal in own plot
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg", subplots = True)

        # dynamic plot each signal in own plot, below each other
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg", subplots = True, shape=(6,1))

        # dynamic plot with rangeslider, not opening in browser
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg", rangeslider=True, open_in_browser=False)

        # dynamic plot with second y axis and rangeslider
        df=dp.signal.plot_signal("bt_dl927_164_luppmenweg",["SNR","battery voltage"], rangeslider=True,secondary_y=["SNR"])


        Other
        -----
        To get to know more about dynamic plotting in python:

        # dynamic
        https://plot.ly/python/
        https://github.com/santosjorge/cufflinks (connector between pandas and plotly)

        # static
        https://matplotlib.org/3.0.2/index.html
        https://pandas.pydata.org/pandas-docs/stable/user_guide/visualization.html

        """

        df = self.__dp.signal.get(
            source_name=source_name,
            variable_name=variable_name,
            start=start,
            end=end,
            show_query=show_query,
        )

        df_reshaped = reshape(df)

        if plot_dynamic:
            fig = df_reshaped.iplot(asFigure=True, **kw_plot_args)

            if inline:
                return df_reshaped, fig
            else:
                _plotly.offline.plot(fig, auto_open=auto_open, filename=filename)

        else:
            df_reshaped.plot(**kw_plot_args)
            _plt.show()

        return df_reshaped

    def plot_signal_with_meta(
        self,
        *,
        variable_name=None,
        source_name=None,
        site_name=None,
        start="1900-01-01 00:00:00",
        end=None,
        to_dataframe=True,
        show_query=False,
        color_encoding={
            "source_installation": "#3a86ff",
            "source_deinstallation": "#3a86ff",
            "source_maintenance": "#8338ec",
            "operational_malfunction": "#e5383b",
            "miscellaneous": "#ffbe0b",
        },
        mark_via_key_word=None,
        minimal_meta_info_with_minutes=10,
        plot_title="Signal Meta Plot",
        filename="meta_plot.html",
        auto_open=True,
        inline=False,
    ):
        """Arguments must be provided with keywords!

        Parameters
        ----------
        source_name:                        Str, specifying the source name
        variable_name:                     str | list, of variable name(s)
        site_name:                          Str, specifying the site name
        start:                              Str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        end:                                Str, specifying a datetime ideally in the format yyyy-mm-dd HH:MM:SS
        to_dataframe:                       bool, specifying whether the query output should be formatted as dataframe
        show_query:                         bool, specifying whether to print the query
        color_encoding:                     dict, specifying plot colors of log types
        mark_via_key_word:                  dict, specifying a keyword to search in meta-string and color for annotating
                                                  with an arrow on the bottom of the plot.
        minimal_meta_info_with_minutes:     Int, setting the minimal width of meta data area plots in minutes
        plot_title:                         Str, title for plot
        filename:                           Str, filename of plot.html if inline is False
        auto_open:                          bool, open plot in browser automatically if inline if False
        inline:                             bool, return the figure so you can open it in your jupyter notebook

        Return
        ------
        signal_data: pandas.DataFrame, meta_data: pd.DataFrame, fig: plotly.Figure if inline=True

        Example
        -------
        from datapool_client import Plot

        dp_plot = Plot() # this only works when a default connection has been set!
        data, meta = dp_plot.plot_with_meta(
            source_name="bl_dl320_597sbw_ara",
            variable_name="water_level",
            start="2017-02-01",
            end="2017-07-01",
        )

        """

        if source_name is None and site_name is None:
            raise ValueError(
                "You must provide at least one of the two: source_name, site_name."
            )

        data = self.__dp.signal.get(
            source_name=source_name,
            site_name=site_name,
            variable_name=variable_name,
            start=start,
            end=end,
            to_dataframe=to_dataframe,
            show_query=show_query,
        )
        meta_data = self.__dp.meta_data_history.get(
            source_name=source_name,
            site_name=site_name,
            start=start,
            end=end,
            to_dataframe=to_dataframe,
            show_query=show_query,
        )

        if meta_data.empty and data.empty:
            print("No data, no meta data available")
            return data, meta_data
        elif data.empty:
            print("No data available")
            return data, meta_data
        elif meta_data.empty:
            print("No meta data available")
            return data, meta_data

        data = reshape(data)
        additional_meta_columns = (
            determine_additional_meta_info_columns_of_meta_data_history(
                meta_data.columns
            )
        )

        meta_data["start"] = pd.to_datetime(meta_data["start"])
        meta_data["end"] = pd.to_datetime(meta_data["end"])

        fig = generate_meta_plot(
            data,
            meta_data,
            additional_meta_columns,
            color_encoding=color_encoding,
            minimal_meta_info_with_minutes=minimal_meta_info_with_minutes,
            plot_title=plot_title,
            filename=filename,
            auto_open=auto_open,
            inline=inline,
            mark_via_key_word=mark_via_key_word
        )
        if inline:
            return data, meta_data, fig
        else:
            return data, meta_data
