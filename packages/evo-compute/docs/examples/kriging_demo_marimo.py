"""Kriging Demo - Marimo Notebook

This notebook demonstrates kriging estimation using the Evo Compute SDK.
"""

import marimo

__generated_with = "0.19.8"
app = marimo.App(width="medium")


@app.cell
def _():
    import numpy as np
    import pandas as pd
    import plotly.graph_objects as go
    import plotly.io as pio
    from plotly.subplots import make_subplots

    import evo.compute.tasks as tasks
    from evo.notebooks import FeedbackWidget, ServiceManagerWidget
    from evo.objects.typed import object_from_path, object_from_uuid

    pio.templates.default = "simple_white"
    return (
        ServiceManagerWidget,
        go,
        make_subplots,
        np,
        object_from_path,
        pd,
        tasks,
    )


@app.cell
async def _(ServiceManagerWidget):
    manager = await ServiceManagerWidget.with_auth_code(
        client_id="core-compute-tasks-notebooks",
        base_uri="https://qa-ims.bentley.com",
        discovery_url="https://int-discover.test.api.seequent.com",
        cache_location="./notebook-data",
    ).login()
    return (manager,)


@app.cell
async def _(manager, object_from_path):
    comps = await object_from_path(manager, "Ag_LMS1 - Ag_ppm Values.json")
    grid = await object_from_path(
        manager, "Kriging Scenarios - 26b90c32-66fb-41e5-987f-3f9a434cda79.json"
    )
    variogram = await object_from_path(
        manager, "Kriging Compute Task/Ag_LMS1: Variogram Model.json"
    )
    return comps, grid, variogram


@app.cell
async def _(comps):
    comps_df = await comps.to_dataframe()
    return (comps_df,)


@app.cell
def _(comps_df, pd):
    composites_stats = pd.DataFrame(comps_df["Ag_ppm Values"].describe())
    composites_stats
    return


@app.cell
def _(comps_df, go):
    comps_hist = go.Histogram(
        x=comps_df["Ag_ppm Values"],
        histnorm="density",
        name="Composites: Ag (ppm)",
        showlegend=True,
    )
    go.Figure(comps_hist)
    return (comps_hist,)


@app.cell
def _(variogram):
    variogram
    return


@app.cell
def _(go, make_subplots, variogram):
    major, semi_major, minor = variogram.get_principal_directions()

    fig_variogram = make_subplots(rows=1, cols=3, horizontal_spacing=0.08)

    for i, direction in enumerate([major, semi_major, minor], 1):
        fig_variogram.add_trace(
            go.Scatter(
                x=direction.distance,
                y=direction.semivariance,
                mode="lines",
                name=direction.direction,
                line={"width": 3},
                showlegend=True,
            ),
            row=1,
            col=i,
        )
    fig_variogram
    return


@app.cell
def _(comps_df, go, np, variogram):
    center = (comps_df.x.mean(), comps_df.y.mean(), comps_df.z.mean())
    ell = variogram.get_ellipsoid()
    srch_ell = ell.scaled(2)
    ell_points = np.array(ell.surface_points(center=center)).T

    ell_mesh = go.Mesh3d(
        x=ell_points[:, 0],
        y=ell_points[:, 1],
        z=ell_points[:, 2],
        alphahull=1,
        opacity=0.5,
        name="Variogram Ellipsoid",
        showlegend=True,
    )

    srch_ell_mesh = go.Mesh3d(
        x=ell_points[:, 0],
        y=ell_points[:, 1],
        z=ell_points[:, 2],
        alphahull=1,
        opacity=0.5,
        name="Search Ellipsoid",
        showlegend=True,
    )

    scatter = go.Scatter3d(
        x=comps_df["x"],
        y=comps_df["y"],
        z=comps_df["z"],
        mode="markers",
        marker={"size": 2, "color": comps_df["Ag_ppm Values"]},
        name="Interval Data",
    )

    go.Figure(
        data=[ell_mesh, srch_ell_mesh, scatter],
        layout=go.Layout(
            title="Kriging Inputs",
            scene={"xaxis_title": "X", "yaxis_title": "Y", "zaxis_title": "Z"},
            showlegend=True,
        ),
    )
    return (ell,)


@app.cell
async def _(comps, ell, grid, manager, tasks, variogram):
    kriging_params = tasks.kriging.KrigingParameters(
        source=comps.attributes["Ag_ppm Values"],
        target=grid.attributes["Silvia"],
        variogram=variogram,
        search=tasks.SearchNeighborhood(
            ellipsoid=ell,
            max_samples=20,
        ),
    )

    estimator_job = await tasks.run(manager, kriging_params)
    return (estimator_job,)


@app.cell
def _(estimator_job):
    estimator_job
    return


@app.cell
async def _(grid):
    grid_refreshed = await grid.refresh()
    return (grid_refreshed,)


@app.cell
async def _(grid_refreshed):
    grid_df = await grid_refreshed.get_data(["Silvia"])
    return (grid_df,)


@app.cell
def _(comps_df, grid_df, pd):
    comparison_stats = pd.DataFrame(
        [comps_df["Ag_ppm Values"].describe(), grid_df["Silvia"].describe()]
    ).T
    comparison_stats
    return


@app.cell
def _(comps_hist, go, grid_df):
    est_hist = go.Histogram(
        x=grid_df["Silvia"],
        histnorm="density",
        name="Estimate: Ag (ppm)",
        showlegend=True,
        nbinsx=50,
    )
    go.Figure([comps_hist, est_hist])
    return


@app.cell
def _(comps, ell, grid, tasks, variogram):
    max_samples_values = [1, 3, 5, 15, 25, 35, 55]
    parameter_sets = []
    for max_samples in max_samples_values:
        params = tasks.kriging.KrigingParameters(
            source=comps.attributes["Ag_ppm Values"],
            target=grid.attributes[f"Silvia_{max_samples}"],
            variogram=variogram,
            search=tasks.SearchNeighborhood(ellipsoid=ell, max_samples=max_samples),
        )
        parameter_sets.append(params)
        print(f"Prepared scenario with max_samples={max_samples}")

    print(f"\nCreated {len(parameter_sets)} parameter sets")
    return (parameter_sets,)


@app.cell
async def _(manager, parameter_sets, tasks):
    results = await tasks.run(manager, parameter_sets)
    return (results,)


@app.cell
def _(results):
    results
    return


@app.cell
async def _(grid):
    grid_final = await grid.refresh()
    return (grid_final,)


@app.cell
def _(grid_final):
    new_cols = [i for i in grid_final.attributes if i.name.startswith("Silvia")]
    return (new_cols,)


@app.cell
async def _(grid_final, new_cols):
    grid_scenarios_df = await grid_final.get_data(columns=[i.name for i in new_cols])
    return (grid_scenarios_df,)


@app.cell
def _(grid_scenarios_df):
    estimates = grid_scenarios_df.drop(["x", "y", "z"], axis=1)
    return (estimates,)


@app.cell
def _(estimates):
    estimates.describe()
    return


@app.cell
def _(estimates, go):
    histograms = []
    for col in estimates.columns:
        histograms.append(
            go.Histogram(
                x=estimates[col],
                histnorm="density",
                name=col,
                showlegend=True,
                opacity=1,
                nbinsx=50,
            )
        )
    go.Figure(histograms)
    return


if __name__ == "__main__":
    app.run()
