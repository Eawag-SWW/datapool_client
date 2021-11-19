from datetime import timedelta

import plotly.graph_objects as go
import plotly.offline as py
from pandas import isna
from plotly.subplots import make_subplots

from datapool_client.core.column_map import COLUMN_MAP


def subdivide_comment(comment, limit=50):
    new_comment = ""
    for tag in comment.split(";"):
        new_tag = ""
        count = 0
        for word in tag.strip().split(" "):
            if count <= limit:
                new_tag += word + " "
            else:
                new_tag += "\n" + word + " "
                count = 0

            count += len(word)

        new_comment += new_tag[:-1] + "\n"

    return new_comment.lstrip(" ")[:-1]


TITLE_NAMES = {
    "header": "Category",
    "timeframe": "Timeframe",
    "comment": "Comment",
    "flag": "Flag",
    "person": "Responsible",
    "add_meta_info": "Additional_Information",
}


def format_title(header):
    return f"<b>{header}</b>\n"


for title in TITLE_NAMES:
    TITLE_NAMES[title] = format_title(TITLE_NAMES[title])


def format_header(log_type, action_type):
    header = f"{TITLE_NAMES['header']}{log_type}"
    if not isna(action_type):
        header += f" -> {action_type}"
    return header


def format_timeframe(start, end):
    return f"{TITLE_NAMES['timeframe']}{start} - {end}"


def format_comment(comments):
    subdivided = subdivide_comment(comments)
    return f"{TITLE_NAMES['comment']}{subdivided}"


def format_flag(flag):
    return f"{TITLE_NAMES['flag']}{flag}"


def format_person(person):
    return f"{TITLE_NAMES['person']}{person}"


def format_additional_meta_info(add_info):
    additional_meta_info = ""
    for key, val in add_info.items():
        additional_meta_info += f"{key}: {val}\n"
    return f"{TITLE_NAMES['add_meta_info']}{additional_meta_info}"


def format_tag(row, additional_meta_info_cols):
    header = format_header(row["log_type"], row["action_type"])
    timeframe = format_timeframe(row["start"], row["end"])
    comments = format_comment(row["comment"])
    flag = format_flag(row["meta_flag"])
    person = format_person(row["person"])
    add_meta_info = format_additional_meta_info(row[additional_meta_info_cols])

    tag_txt = f"{header}\n\n{timeframe}\n\n{comments}\n\n{flag}\n\n{person}\n\n{add_meta_info}"

    return tag_txt.replace("\n", "<br>")


def determine_additional_meta_info_columns_of_meta_data_history(meta):
    meta_cols = COLUMN_MAP["meta_data_history_get"]
    return list(set(meta.columns) - set(meta_cols))


def generate_meta_plot(
    dataframe,
    meta,
    additional_meta_info_cols,
    color_encoding,
    minimal_meta_info_with_minutes=10,
    plot_title="Signal Meta Plot",
    filename="meta_plot.html",
    auto_open=True,
    inline=False,
):

    values = dataframe.values

    mx = values.max()
    meta_rows = meta.shape[0]

    annotations = []
    highlighted_area = []
    for i, (_, row) in enumerate(meta.iterrows()):

        st = row["start"]
        en = row["end"]

        row_formatted = format_tag(row, additional_meta_info_cols)

        if st == en:
            en += timedelta(minutes=minimal_meta_info_with_minutes)

        low = i / meta_rows
        high = (i + 1) / meta_rows

        highlighted_area.append(
            go.Scatter(
                x=[st, st, en, en, st],
                y=[low, high, high, low, low],
                showlegend=False,
                name=f"meta_info_{i}",
                marker=dict(color=color_encoding[row["log_type"]], opacity=0.0),
                fill="toself",
                fillcolor=color_encoding[row["log_type"]],
                opacity=0.3,
                line={"width": 1, "color": "black"},
                text=row_formatted,
                hoveron="fills",
                hovertemplate="<b>%{text} </b><br><br>" + "<extra></extra>",
            )
        )

        annotations.append(
            dict(
                x=row["start"],
                y=mx,
                textangle=90,
                ax=0,
                ay=-20,
                font=dict(color=color_encoding[row["log_type"]], size=12),
                arrowcolor=color_encoding[row["log_type"]],
                arrowsize=1,
                arrowwidth=1,
                arrowhead=1,
            )
        )

    fig = make_subplots(
        rows=2,
        cols=1,
        row_heights=[0.9, 0.1],
        shared_xaxes=True,
    )

    for trace in highlighted_area:
        fig.add_trace(trace, row=2, col=1)

    for param in dataframe.columns:
        fig.add_trace(
            go.Scatter(
                x=dataframe.index,
                y=dataframe[param].values,
                name=param,
                #line=dict(color=line_color, width=1.5),
            ),
            row=1,
            col=1,
        )

    fig.update_layout(
        title={
            "text": f'<span style="font-size: 20px;"><b>{plot_title}</b></span>',
            "y": 0.95,
            "x": 0.5,
        },
        annotations=annotations,
        yaxis2=dict(domain=[0, 0.12]),
        yaxis1=dict(domain=[0.14, 1]),
    )

    fig.update_yaxes(visible=False, row=2, col=1)

    if inline:
        return fig

    else:
        py.plot(fig, filename=filename, auto_open=auto_open)
