# app/perf_logic.py
from __future__ import annotations
import os
import pandas as pd
from collections import defaultdict
from pathlib import Path
from datetime import datetime

# Puedes dejarlo como fallback/manual si algún día quieres forzar un orden
DEFAULT_STATION_ORDER = {
    "fto": 1,
    "runnin": 2,
    "test4": 3,
    "disk test": 4,
    "swap": 0,   # laboratorio, fuera del flujo
}


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["sn"] = df["sn"].astype("string")
    df["station"] = df["station"].str.strip().str.lower()
    df["status"]  = df["status"].str.strip().str.lower()
    df["badge"]   = df["badge"].astype(str)

    # 👇 nuevo
    if "error_code" in df.columns:
        df["error_code"] = (
            df["error_code"]
            .astype("string")
            .str.strip()
            .str.lower()
        )
    else:
        df["error_code"] = ""

    df["station"] = df["station"].astype("category")
    df["status"]  = df["status"].astype("category")
    df["badge"]   = df["badge"].astype("category")
    return df


def _infer_station_order(df: pd.DataFrame) -> dict[str, int]:
    """
    Construye un orden lineal de estaciones a partir del flujo real del CSV.

    - Recorre todas las filas en orden y agrega estaciones sin repetir.
    - 'swap' se fuerza a rank 0 (laboratorio / fuera de flujo).
    - El resto se numera 1..N en el orden en que aparecen.
    """
    seen: list[str] = []
    has_swap = False

    for st in df["station"]:
        if st == "swap":
            has_swap = True
            continue
        if st not in seen:
            seen.append(st)

    station_order: dict[str, int] = {st: i + 1 for i, st in enumerate(seen)}
    if has_swap:
        station_order["swap"] = 0
    return station_order


def compute_performance_from_df(
    df: pd.DataFrame,
    station_order: dict[str, int] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """Devuelve (df_result, meta) sin tocar disco.

    - Si station_order es None, se infiere automáticamente del CSV.
    - Evalúa la reparación por (station, error_code):
        * Si después del swap vuelve a fallar MISMA estación + MISMO error_code → ineficiencia (no éxito).
        * Si vuelve a fallar MISMA estación pero OTRO error_code → la reparación anterior fue exitosa, problema nuevo.
        * Si falla más adelante en otra estación → reparación exitosa, problema nuevo.
    """
    df = _normalize_frame(df)

    # Si no pasas station_order, lo inferimos desde el CSV
    if station_order is None:
        station_order = _infer_station_order(df)

    # Por si alguna vez station_order viene vacío/incorrecto
    max_flow_rank = max((r for r in station_order.values() if r > 0), default=0)

    # Orden temporal: si no hay timestamp, usamos el orden del archivo
    if "event_id" not in df.columns:
        df = df.reset_index().rename(columns={"index": "event_id"})
    df = df.sort_values(["sn", "event_id"], kind="mergesort")

    intentos: dict[str, int] = defaultdict(int)
    exitos: dict[str, int] = defaultdict(int)
    total_fallas = 0

    # Recorremos unidad por unidad
    for sn, g in df.groupby("sn", sort=False):
        rows = g.to_dict("records")
        i = 0
        while i < len(rows):
            r = rows[i]
            st = r["station"]
            st_status = r["status"]

            # fallas en estaciones del flujo (rank > 0)
            if (st_status == "fail") and (st in station_order) and (station_order[st] > 0):
                total_fallas += 1
                fail_rank = station_order[st]
                orig_error = r.get("error_code", "")  # 👈 error original en esa estación

                # buscar el siguiente swap
                j = i + 1
                swap_row = None
                while j < len(rows):
                    if rows[j]["station"] == "swap":
                        swap_row = rows[j]
                        break
                    j += 1

                # si nunca llega a swap, no podemos atribuir a técnico
                if swap_row is None:
                    i += 1
                    continue

                badge = swap_row.get("badge")
                if not badge:
                    i = j + 1
                    continue

                # 1 intento para ese técnico
                intentos[badge] += 1

                # Ahora analizamos lo que pasa DESPUÉS del swap
                success = False
                max_pass_rank = -1
                k = j + 1
                while k < len(rows):
                    st2 = rows[k]["station"]
                    if st2 == "swap":
                        # Nuevo técnico: hasta aquí llega este intento
                        break

                    status2 = rows[k]["status"]
                    rank2 = station_order.get(st2, -1)

                    if status2 == "fail":
                        err2 = rows[k].get("error_code", "")
                        same_code = (orig_error and err2 and err2 == orig_error)
                        same_station = (st2 == st)  # 👈 MISMA estación que la falla original

                        # 🔴 Solo cuenta como ineficiencia si:
                        #    - es la MISMA estación   (st2 == st)
                        #    - y el MISMO error_code (err2 == orig_error)
                        if same_station and same_code:
                            # MISMA estación + MISMO error → mala reparación
                            success = False
                            max_pass_rank = -1
                            break
                        else:
                            # estación diferente o error diferente:
                            # → reparación correcta, falla nueva (otro problema)
                            success = True
                            break


                    if status2 == "pass":
                        if rank2 > max_pass_rank:
                            max_pass_rank = rank2
                        # Si ya pasó la estación donde falló → éxito
                        if max_pass_rank >= fail_rank:
                            success = True
                        # Si ya llegó al final del flujo, cortamos
                        if max_pass_rank == max_flow_rank:
                            break

                    k += 1

                if success:
                    exitos[badge] += 1

                # seguimos a partir del swap (nuevo intento/problema)
                i = j + 1
                continue

            # si no entró en el caso de falla, avanzamos
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
        score = round(ex + 0.5 * carga_rel, 3)
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
            df_result.sort_values(
                by=["score", "exitos", "intentos"],
                ascending=[False, False, False],
            )
            .reset_index(drop=True)
        )

    meta = {
        "fallas_totales": total_fallas,
        "tecnicos_activos": tecnicos_activos,
        "carga_esperada": carga_esperada,
        "station_order": station_order,
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

    cols = ["badge","intentos","exitos","ineficiencias","eficiencia_%","carga_relativa","score"]

    df_print = df_result.copy()
    df_print["eficiencia_%"]   = df_print["eficiencia_%"].apply(lambda x: fmt_float(x, 2))
    df_print["carga_relativa"] = df_print["carga_relativa"].apply(lambda x: fmt_float(x, 3))
    df_print["score"]          = df_print["score"].apply(lambda x: fmt_float(x, 3))

    right_align = {"intentos","exitos","ineficiencias","eficiencia_%","carga_relativa","score"}
    left_align  = set(cols) - right_align

    widths = {}
    for c in cols:
        head_len = len(c)
        body_len = df_print[c].astype(str).map(len).max()
        widths[c] = max(head_len, body_len)

    def align(val, col):
        s = str(val)
        w = widths[col]
        if col in right_align:
            return s.rjust(w)
        return s.ljust(w)

    header = "  ".join(align(c, c) for c in cols)
    sep    = "  ".join("-" * widths[c] for c in cols)
    lines.append(header)
    lines.append(sep)

    for _, row in df_print.iterrows():
        line = "  ".join(align(row[c], c) for c in cols)
        lines.append(line)

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
