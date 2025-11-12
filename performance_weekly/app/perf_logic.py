# app/perf_logic.py
from __future__ import annotations
import os
import pandas as pd
from collections import defaultdict
from pathlib import Path
from datetime import datetime

DEFAULT_STATION_ORDER = {
    "fto": 1,
    "runnin": 2,      # si tu CSV dice "runnin", respeta eso
    "test4": 3,
    "disk test": 4,
    "swap": 0,        # laboratorio, fuera del flujo
}

def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    # Normaliza/castea – no tiramos columnas extra si llegan
    df = df.copy()
    df["sn"] = df["sn"].astype("string")
    df["station"] = df["station"].str.strip().str.lower()
    df["status"]  = df["status"].str.strip().str.lower()
    df["badge"]   = df["badge"].astype(str)
    # tipos compactos
    df["station"] = df["station"].astype("category")
    df["status"]  = df["status"].astype("category")
    df["badge"]   = df["badge"].astype("category")
    return df

def compute_performance_from_df(
    df: pd.DataFrame,
    station_order: dict[str,int] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Devuelve (df_result, meta) sin tocar disco."""
    station_order = station_order or DEFAULT_STATION_ORDER
    max_flow_rank = max(r for r in station_order.values() if r > 0)

    df = _normalize_frame(df)
    # Orden temporal: si no hay timestamp, usamos el orden del archivo
    if "event_id" not in df.columns:
        df = df.reset_index().rename(columns={"index": "event_id"})
    df = df.sort_values(["sn", "event_id"], kind="mergesort")

    intentos = defaultdict(int)
    exitos   = defaultdict(int)
    total_fallas = 0

    for sn, g in df.groupby("sn", sort=False):
        rows = g.to_dict("records")
        i = 0
        while i < len(rows):
            r = rows[i]
            st = r["station"]
            st_status = r["status"]
            if (st_status == "fail") and (st in station_order) and (station_order[st] > 0):
                total_fallas += 1
                fail_rank = station_order[st]

                # buscar el siguiente swap
                j = i + 1
                swap_row = None
                while j < len(rows):
                    if rows[j]["station"] == "swap":
                        swap_row = rows[j]
                        break
                    j += 1

                if swap_row is None:
                    i += 1
                    continue

                badge = swap_row.get("badge")
                if not badge:
                    i = j + 1
                    continue

                intentos[badge] += 1

                # post-swap
                success = False
                max_pass_rank = -1
                k = j + 1
                while k < len(rows):
                    st2 = rows[k]["station"]
                    if st2 == "swap":
                        break

                    status2 = rows[k]["status"]
                    rank2 = station_order.get(st2, -1)

                    if status2 == "fail":
                        # si falla igual o antes → mala reparación
                        if rank2 <= fail_rank:
                            success = False
                            max_pass_rank = -1
                            break
                        else:
                            # falló más adelante → reparación buena (nuevo problema)
                            success = True
                            break

                    if status2 == "pass":
                        if rank2 > max_pass_rank:
                            max_pass_rank = rank2
                        if max_pass_rank >= fail_rank:
                            success = True
                        if max_pass_rank == max_flow_rank:
                            break

                    k += 1

                if success:
                    exitos[badge] += 1

                i = j + 1
                continue

            i += 1

    # métricas de carga
    tecnicos_activos = len(intentos)
    carga_esperada = total_fallas / tecnicos_activos if tecnicos_activos else 0

    rows_out = []
    for badge in intentos:
        it = intentos[badge]
        ex = exitos[badge]
        inef = it - ex
        ef_pct = round(ex / it * 100, 2) if it else 0
        carga_rel = round(it / carga_esperada, 3) if carga_esperada else 0
        score = round(ex + 0.5 * carga_rel, 3)   # puedes ajustar el peso
        rows_out.append({
            "badge": badge,
            "intentos": it,
            "exitos": ex,
            "ineficiencias": inef,
            "eficiencia_%": ef_pct,
            "carga_relativa": carga_rel,
            "score": score,
        })

    df_result = pd.DataFrame(rows_out)
    if not df_result.empty:
        df_result = (
            df_result.sort_values(by=["score", "exitos", "intentos"], ascending=[False, False, False])
                     .reset_index(drop=True)
        )

    meta = {
        "fallas_totales": total_fallas,
        "tecnicos_activos": tecnicos_activos,
        "carga_esperada": carga_esperada,
    }
    return df_result, meta

def run_performance_from_csv(csv_path: str, station_order: dict[str,int] | None = None) -> tuple[pd.DataFrame, dict]:
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    return compute_performance_from_df(df, station_order=station_order)

def write_txt_report(txt_path: str, df_result: pd.DataFrame, meta: dict, title: str):
    Path(txt_path).parent.mkdir(parents=True, exist_ok=True)

    def fmt_float(val, nd=2):
        try:
            return f"{float(val):.{nd}f}"
        except Exception:
            return str(val)

    lines = []
    lines.append(f"{title}")
    lines.append("-" * len(title))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"Generado: {now}")
    lines.append("")

    lines.append(f"Fallas totales: {meta.get('fallas_totales', 0)}")
    lines.append(f"Técnicos activos: {meta.get('tecnicos_activos', 0)}")
    carga = meta.get('carga_esperada', 0) or 0
    lines.append(f"Carga esperada por técnico: {carga:.3f}")
    lines.append("")

    if df_result is None or df_result.empty:
        lines.append("No hubo métricas para mostrar.")
        with open(txt_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        return

    # --- tabla alineada ---
    cols = ["badge","intentos","exitos","ineficiencias","eficiencia_%","carga_relativa","score"]

    # preformatear números
    df_print = df_result.copy()
    df_print["eficiencia_%"]   = df_print["eficiencia_%"].apply(lambda x: fmt_float(x, 2))
    df_print["carga_relativa"] = df_print["carga_relativa"].apply(lambda x: fmt_float(x, 3))
    df_print["score"]          = df_print["score"].apply(lambda x: fmt_float(x, 3))

    # tipos de alineación por columna
    right_align = {"intentos","exitos","ineficiencias","eficiencia_%","carga_relativa","score"}
    left_align  = set(cols) - right_align

    # calcular ancho por columna (considerando encabezado y filas)
    widths = {}
    for c in cols:
        head_len = len(c)
        body_len = df_print[c].astype(str).map(len).max()
        widths[c] = max(head_len, body_len)

    # helpers de alineación
    def align(val, col):
        s = str(val)
        w = widths[col]
        if col in right_align:
            return s.rjust(w)
        return s.ljust(w)

    # encabezado
    header = "  ".join(align(c, c) for c in cols)
    sep    = "  ".join("-" * widths[c] for c in cols)
    lines.append(header)
    lines.append(sep)

    # filas
    for _, row in df_print.iterrows():
        line = "  ".join(align(row[c], c) for c in cols)
        lines.append(line)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))