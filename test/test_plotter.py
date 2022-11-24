import matplotlib
import pytest

matplotlib.use("Agg")


def test_plot_with_meta_raises(setup_postgres, plot):
    with pytest.raises(ValueError):
        plot.plot_signal_with_meta(
            variable_name="variable_1",
        )


def test_plot_inline_no_variable(setup_postgres, plot):
    data, fig = plot.plot_signal(
        source_name="source_1_1",
        inline=True,
    )


def test_plot_inline(setup_postgres, plot):
    data, fig = plot.plot_signal(
        source_name="source_1_1",
        variable_name="variable_1",
        inline=True,
    )


def test_plot_not_inline(setup_postgres, plot, tmp_path):
    data = plot.plot_signal(
        source_name="source_1_1",
        variable_name="variable_1",
        auto_open=False,
        filename=str(tmp_path / "test.html"),
    )


def test_plot_dynamic_kwargs(setup_postgres, plot, tmp_path):
    data = plot.plot_signal(
        source_name="source_1_1",
        variable_name="variable_1",
        auto_open=False,
        rangeslider=True,
        filename=str(tmp_path / "test.html"),
    )


def test_plot_static_kwargs(setup_postgres, plot, tmp_path):
    data = plot.plot_signal(source_name="source_1_1", plot_dynamic=False, title="title")


def test_plot_static(setup_postgres, plot, tmp_path):
    data = plot.plot_signal(
        source_name="source_1_1", variable_name="variable_1", plot_dynamic=False
    )


def test_plot_list_of_variables(setup_postgres, plot, tmp_path):
    data = plot.plot_signal(
        source_name="source_1_1",
        variable_name=["variable_1", "variable_2"],
        auto_open=False,
        filename=str(tmp_path / "test.html"),
    )


def test_plot_with_meta_inline(setup_postgres, plot):
    data, meta, fig = plot.plot_signal_with_meta(
        source_name="source_1_1",
        variable_name="variable_1",
        site_name="site_1",
        inline=True,
    )


def test_plot_with_meta_not_inline(setup_postgres, plot, tmp_path):
    data, meta = plot.plot_signal_with_meta(
        source_name="source_1_1",
        variable_name="variable_1",
        site_name="site_1",
        filename=str(tmp_path / "plot_meta.html"),
        auto_open=False,
    )


def test_plot_with_meta_not_inline_variable_list(setup_postgres, plot, tmp_path):
    data, meta = plot.plot_signal_with_meta(
        source_name="source_1_1",
        variable_name=["variable_1", "variable_2"],
        site_name="site_1",
        filename=str(tmp_path / "plot_meta_multi_param.html"),
        auto_open=False,
    )


def test_plot_with_meta_only_site(setup_postgres, plot, tmp_path):
    data, meta = plot.plot_signal_with_meta(
        site_name="site_1",
        filename=str(tmp_path / "plot_meta_site.html"),
        auto_open=False,
    )
