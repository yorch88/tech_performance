# tests/test_perf_logic.py
from io import StringIO
import pandas as pd

from app.perf_logic import compute_performance_from_df


SAMPLE_CSV = """sn,station,status,error_code,badge
mxq1,fto,pass,,123
mxq1,runnin,pass,,123
mxq1,test4,fail,0-A23,123
mxq1,swap,pass,,456
mxq1,fto,pass,,456
mxq1,runnin,pass,,456
mxq1,test4,pass,,456
mxq1,disk test,pass,,456
mxq1,crisscross,pass,,456
mxq2,fto,pass,,789
mxq2,runnin,pass,,789
mxq2,test4,pass,,789
mxq2,disk test,fail,0-A23,789
mxq2,swap,pass,,456
mxq2,fto,pass,,456
mxq2,runnin,fail,0-A23,456
mxq2,swap,pass,,987
mxq2,fto,pass,,987
mxq2,runnin,pass,,987
mxq2,test4,pass,,987
mxq2,disk test,pass,,987
mxq3,fto,pass,,111
mxq3,runnin,fail,0-A23,111
mxq3,swap,pass,,987
mxq3,fto,fail,0-A23,987
mxq3,swap,pass,,987
mxq3,fto,pass,,987
mxq3,runnin,pass,,987
mxq3,test4,fail,0-A23,987
mxq3,swap,pass,,333
mxq3,fto,pass,,333
mxq3,runnin,pass,,333
mxq3,test4,pass,,333
mxq3,disk test,pass,,333
mxq4,fto,fail,0-A23,222
mxq4,swap,pass,,555
mxq4,fto,fail,0-A23,555
mxq4,swap,pass,,555
mxq4,fto,fail,0-A23,555
mxq4,swap,pass,,666
mxq4,fto,pass,,666
mxq4,runnin,pass,,666
mxq4,test4,pass,,666
mxq4,disk test,pass,,666
mxq5,fto,pass,,987
mxq5,runnin,fail,0-A23,987
mxq5,swap,pass,,987
mxq5,fto,pass,,987
mxq5,runnin,pass,,987
mxq5,test4,fail,0-A23,987
mxq5,swap,pass,,777
mxq5,fto,pass,,777
mxq5,runnin,pass,,777
mxq5,test4,pass,,777
mxq5,disk test,pass,,777
"""


def load_sample_df() -> pd.DataFrame:
    """Construye un DataFrame a partir del CSV de ejemplo."""
    return pd.read_csv(StringIO(SAMPLE_CSV), dtype=str, keep_default_na=False)


def df_by_badge(df_result: pd.DataFrame) -> dict:
    """Indexa el resultado por badge para facilitar asserts."""
    return {
        str(row["badge"]): row
        for _, row in df_result.iterrows()
    }


def test_metrics_for_sample_week():
    df = load_sample_df()
    df_result, meta = compute_performance_from_df(df)

    by_badge = df_by_badge(df_result)

    # --- métricas globales ---
    assert meta["fallas_totales"] == 11
    assert meta["tecnicos_activos"] == 6
    # 11 fallas / 6 técnicos = 1.8333...
    assert round(meta["carga_esperada"], 3) == round(11 / 6, 3)

    # --- badge 987: 4 intentos, 4 éxitos, 0 ineficiencias ---
    b987 = by_badge["987"]
    assert b987["intentos"] == 4
    assert b987["exitos"] == 4
    assert b987["ineficiencias"] == 0

    # --- badge 456: 2 intentos, 2 éxitos, 0 ineficiencias ---
    b456 = by_badge["456"]
    assert b456["intentos"] == 2
    assert b456["exitos"] == 2
    assert b456["ineficiencias"] == 0

    # --- badge 555: 2 intentos, 0 éxitos, 2 ineficiencias ---
    b555 = by_badge["555"]
    assert b555["intentos"] == 2
    assert b555["exitos"] == 0
    assert b555["ineficiencias"] == 2

    # --- algunos sanity checks extra ---
    # Ningún técnico debe tener ineficiencias negativas
    for row in df_result.to_dict(orient="records"):
        assert row["ineficiencias"] >= 0

    # El orden de estaciones inferido debe incluir swap=0 y flujo lineal
    station_order = meta["station_order"]
    assert station_order["swap"] == 0
    # fto < runnin < test4 < disk test
    assert station_order["fto"] < station_order["runnin"] < station_order["test4"] < station_order["disk test"]
