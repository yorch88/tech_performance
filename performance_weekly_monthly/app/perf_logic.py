# app/perf_logic.py
from __future__ import annotations

import pandas as pd
from collections import defaultdict
from typing import Iterable, Dict, Tuple, List
from io import StringIO
from pathlib import Path
from datetime import datetime
from .report_models import FailureEvent, RepairAttempt  # 👈 import relativo


# --------------------------
# Normalización y orden
# --------------------------


def _normalize_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Normaliza tipos y columnas básicas del CSV."""
    df = df.copy()
    df["sn"] = df["sn"].astype("string")
    df["station"] = df["station"].str.strip().str.lower()
    df["status"] = df["status"].str.strip().str.lower()
    df["badge"] = df["badge"].astype(str)

    # error_code puede venir vacío / faltante
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
    df["status"] = df["status"].astype("category")
    df["badge"] = df["badge"].astype("category")
    return df


def _infer_station_order(df: pd.DataFrame) -> Dict[str, int]:
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

    station_order: Dict[str, int] = {st: i + 1 for i, st in enumerate(seen)}
    if has_swap:
        station_order["swap"] = 0
    return station_order


def _prepare_dataframe(
    df: pd.DataFrame,
    station_order: Dict[str, int] | None,
) -> Tuple[pd.DataFrame, Dict[str, int], int]:
    """
    - Normaliza columnas
    - Infere station_order si es None
    - Asegura orden temporal por sn + event_id
    - Devuelve (df_ordenado, station_order, max_flow_rank)
    """
    df = _normalize_frame(df)

    if station_order is None:
        station_order = _infer_station_order(df)

    max_flow_rank = max((r for r in station_order.values() if r > 0), default=0)

    if "event_id" not in df.columns:
        df = df.reset_index().rename(columns={"index": "event_id"})
    df = df.sort_values(["sn", "event_id"], kind="mergesort")

    return df, station_order, max_flow_rank


# --------------------------
# Iteradores de dominio
# --------------------------


def _iter_failures_for_sn(
    sn: str,
    rows: List[dict],
    station_order: Dict[str, int],
) -> Iterable[FailureEvent]:
    """
    Itera por todas las fallas relevantes (en estaciones del flujo, rank > 0)
    para un número de serie (sn).
    """
    i = 0
    while i < len(rows):
        r = rows[i]
        st = r["station"]
        st_status = r["status"]

        if (st_status == "fail") and (st in station_order) and (station_order[st] > 0):
            yield FailureEvent(
                sn=sn,
                fail_index=i,
                station=st,
                error_code=r.get("error_code", "") or "",
                fail_rank=station_order[st],
            )
        i += 1


def _find_repair_attempt(
    failure: FailureEvent,
    rows: List[dict],
) -> RepairAttempt | None:
    """
    A partir de una falla, busca el siguiente 'swap' y devuelve el intento de reparación.
    Si no hay swap o no hay badge, devuelve None.
    """
    j = failure.fail_index + 1
    swap_row = None

    while j < len(rows):
        if rows[j]["station"] == "swap":
            swap_row = rows[j]
            break
        j += 1

    if swap_row is None:
        return None

    badge = swap_row.get("badge")
    if not badge:
        return None

    return RepairAttempt(
        technician_badge=str(badge),
        failure=failure,
        swap_index=j,
    )


def _evaluate_repair_attempt(
    attempt: RepairAttempt,
    rows: List[dict],
    station_order: Dict[str, int],
    max_flow_rank: int,
) -> bool:
    """
    Devuelve True si el intento del técnico es exitoso, False si es ineficiente.

    Reglas:
    - MISMA estación + MISMO error_code después del swap → ineficiencia.
    - Estación diferente o código diferente → reparación correcta, problema nuevo.
    - Si la unidad pasa la estación de la falla original (rank >= fail_rank) y
      no se repite la combinación (misma estación + mismo error), también es éxito.
    """
    failure = attempt.failure
    st_fail = failure.station
    orig_error = failure.error_code
    fail_rank = failure.fail_rank

    success = False
    max_pass_rank = -1

    k = attempt.swap_index + 1
    while k < len(rows):
        st2 = rows[k]["station"]

        # Nuevo swap → termina el alcance de este intento
        if st2 == "swap":
            break

        status2 = rows[k]["status"]
        rank2 = station_order.get(st2, -1)

        if status2 == "fail":
            err2 = rows[k].get("error_code", "") or ""
            same_code = bool(orig_error) and bool(err2) and (err2 == orig_error)
            same_station = (st2 == st_fail)

            # MISMA estación + MISMO código → mala reparación
            if same_station and same_code:
                success = False
                max_pass_rank = -1
                break
            else:
                # Nueva falla (estación distinta o código distinto)
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

    return success


def _build_metrics_dataframe(
    attempts: Dict[str, int],
    successes: Dict[str, int],
    total_failures: int,
) -> tuple[pd.DataFrame, dict]:
    tecnicos_activos = len(attempts)
    carga_esperada = total_failures / tecnicos_activos if tecnicos_activos else 0

    rows_out = []
    for badge, it in attempts.items():
        ex = successes.get(badge, 0)
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
        "fallas_totales": total_failures,
        "tecnicos_activos": tecnicos_activos,
        "carga_esperada": carga_esperada,
    }
    return df_result, meta


# --------------------------
# API principal
# --------------------------


def compute_performance_from_df(
    df: pd.DataFrame,
    station_order: dict[str, int] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Orquesta el cálculo de performance:

    1. Prepara DataFrame y orden de estaciones.
    2. Recorre cada unidad (sn) y detecta fallas relevantes.
    3. Para cada falla:
        - encuentra el intento de reparación (swap + técnico)
        - evalúa si fue exitoso o ineficiente
    4. Construye el DataFrame final de métricas + meta.
    """
    df, station_order, max_flow_rank = _prepare_dataframe(df, station_order)

    attempts: Dict[str, int] = defaultdict(int)
    successes: Dict[str, int] = defaultdict(int)
    total_failures = 0

    # 1) Recorremos unidad por unidad
    for sn, g in df.groupby("sn", sort=False):
        rows = g.to_dict("records")

        # 2) Iterar fallas relevantes en esta unidad
        for failure in _iter_failures_for_sn(sn, rows, station_order):
            total_failures += 1

            # 3) Encontrar intento de reparación
            attempt = _find_repair_attempt(failure, rows)
            if attempt is None:
                continue

            badge = attempt.technician_badge
            attempts[badge] += 1

            # 4) Evaluar éxito / ineficiencia
            success = _evaluate_repair_attempt(
                attempt=attempt,
                rows=rows,
                station_order=station_order,
                max_flow_rank=max_flow_rank,
            )
            if success:
                successes[badge] += 1

    # 5) Construir métricas
    df_result, meta = _build_metrics_dataframe(
        attempts=attempts,
        successes=successes,
        total_failures=total_failures,
    )

    # Añadimos station_order para debug/trazabilidad
    meta["station_order"] = station_order
    return df_result, meta


def run_performance_from_csv(
    csv_path: str,
    station_order: dict[str, int] | None = None,
) -> tuple[pd.DataFrame, dict]:
    """
    Helper para compatibilidad con tasks.py:
    - Lee el CSV
    - Llama a compute_performance_from_df
    """
    df = pd.read_csv(csv_path, dtype=str, keep_default_na=False)
    return compute_performance_from_df(df, station_order=station_order)


def write_txt_report(
    txt_path: str,
    df_result: pd.DataFrame,
    meta: dict,
    title: str,
) -> None:
    """
    Genera un reporte de texto alineado con métricas por técnico.
    """
    Path(txt_path).parent.mkdir(parents=True, exist_ok=True)

    def fmt_float(val, nd=2):
        try:
            return f"{float(val):.{nd}f}"
        except Exception:
            return str(val)

    lines: list[str] = []
    lines.append(title)
    lines.append("-" * len(title))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    lines.append(f"Generado: {now}")
    lines.append("")

    lines.append(f"Fallas totales: {meta.get('fallas_totales', 0)}")
    lines.append(f"Técnicos activos: {meta.get('tecnicos_activos', 0)}")
    carga = meta.get("carga_esperada", 0) or 0
    lines.append(f"Carga esperada por técnico: {carga:.3f}")
    lines.append("")

    if df_result is None or df_result.empty:
        lines.append("No hubo métricas para mostrar.")
        Path(txt_path).write_text("\n".join(lines), encoding="utf-8")
        return

    # columnas en el orden deseado
    cols = ["badge", "intentos", "exitos", "ineficiencias", "eficiencia_%", "carga_relativa", "score"]

    df_print = df_result.copy()
    df_print["eficiencia_%"] = df_print["eficiencia_%"].apply(lambda x: fmt_float(x, 2))
    df_print["carga_relativa"] = df_print["carga_relativa"].apply(lambda x: fmt_float(x, 3))
    df_print["score"] = df_print["score"].apply(lambda x: fmt_float(x, 3))

    right_align = {"intentos", "exitos", "ineficiencias", "eficiencia_%", "carga_relativa", "score"}
    left_align = set(cols) - right_align

    # calcular anchos
    widths: dict[str, int] = {}
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
    sep = "  ".join("-" * widths[c] for c in cols)
    lines.append(header)
    lines.append(sep)

    for _, row in df_print.iterrows():
        line = "  ".join(align(row[c], c) for c in cols)
        lines.append(line)

    Path(txt_path).write_text("\n".join(lines), encoding="utf-8")