import os
import sys
import time
import logging
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

DB_USER = os.getenv('DB_USER', 'root')
DB_PASS = os.getenv('DB_PASS', 'Lolololo060905**')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '3306')
DB_NAME = os.getenv('DB_NAME', 'andino')

DATA_DIR = "data"
EXCEL_FILE = os.path.join(DATA_DIR, "plantillas_andino.xlsx")
CSV_FILES = {
    "aerolinea": os.path.join(DATA_DIR, "aerolinea.csv"),
    "aeronave": os.path.join(DATA_DIR, "aeronave.csv"),
    "terminal": os.path.join(DATA_DIR, "terminal.csv"),
    "puerta": os.path.join(DATA_DIR, "puerta.csv"),
    "vuelo": os.path.join(DATA_DIR, "vuelo.csv"),
    "pasajero": os.path.join(DATA_DIR, "pasajero.csv"),
    "reserva": os.path.join(DATA_DIR, "reserva.csv"),
    # opcionales
    "pase_abordar": os.path.join(DATA_DIR, "pase_abordar.csv"),
    "equipaje": os.path.join(DATA_DIR, "equipaje.csv"),
    "evento_embarque": os.path.join(DATA_DIR, "evento_embarque.csv"),
    "log_cambios": os.path.join(DATA_DIR, "log_cambios.csv"),
}

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

engine = create_engine(
    f'mysql+pymysql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}?charset=utf8mb4',
    pool_pre_ping=True,
    pool_recycle=3600
)

def read_sheet_or_csv(name, sheet_name=None):
    csv_path = CSV_FILES.get(name)
    if csv_path and os.path.exists(csv_path):
        try:
            df = pd.read_csv(csv_path, dtype=str, keep_default_na=False, na_values=[''])
            log.info("Leído CSV %s (%d filas).", csv_path, len(df))
            return df
        except Exception as e:
            log.warning("Error leyendo CSV %s: %s", csv_path, e)
    if os.path.exists(EXCEL_FILE):
        try:
            sheet = name if sheet_name is None else sheet_name
            df = pd.read_excel(EXCEL_FILE, sheet_name=sheet, engine='openpyxl', dtype=str)
            log.info("Leído hoja '%s' de %s (%d filas).", sheet, EXCEL_FILE, len(df))
            return df
        except ImportError:
            log.error("Falta openpyxl. Instala con: python -m pip install openpyxl")
        except Exception as e:
            log.warning("No se pudo leer hoja '%s' en %s: %s", name, EXCEL_FILE, e)
    log.debug("No existe CSV ni hoja para '%s'. Se considerará vacía.", name)
    return pd.DataFrame()

def normalize_df(df):
    if df is None or df.empty:
        return pd.DataFrame()
    return df.replace({'': None}).where(pd.notnull(df), None)

def _to_int_safe(val):
    if val is None:
        return None
    if isinstance(val, (int,)):
        return val
    try:
        s = str(val).strip()
        if s == '':
            return None
        return int(float(s))
    except Exception:
        return None

def _to_bool_safe(val):
    if val is None:
        return None
    s = str(val).strip().lower()
    if s in ('1','true','t','yes','y'):
        return True
    if s in ('0','false','f','no','n'):
        return False
    return None

def insert_row_if_missing(conn, table, row, pk='id'):
    """
    Inserta row (dict) en table solo si no existe id = row[pk].
    Si id no provisto, inserta sin id (auto_increment).
    Retorna True si insertó, False si ya existía o no hubo columnas.
    """
    row = {k: (None if pd.isna(v) else v) for k, v in row.items()}
    if pk in row:
        row[pk] = _to_int_safe(row[pk])
    cols_nonnull = {k: v for k, v in row.items() if v is not None}
    if pk not in cols_nonnull:
        if not cols_nonnull:
            return False
        cols = list(cols_nonnull.keys())
        vals = {c: cols_nonnull[c] for c in cols}
        cols_sql = ", ".join(cols)
        placeholders = ", ".join([f":{c}" for c in cols])
        sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
        conn.execute(text(sql), vals)
        return True
    else:
        exists = conn.execute(text(f"SELECT 1 FROM {table} WHERE id = :id LIMIT 1"), {"id": cols_nonnull[pk]}).fetchone()
        if exists:
            return False
        cols = list(cols_nonnull.keys())
        vals = {c: cols_nonnull[c] for c in cols}
        cols_sql = ", ".join(cols)
        placeholders = ", ".join([f":{c}" for c in cols])
        sql = f"INSERT INTO {table} ({cols_sql}) VALUES ({placeholders})"
        conn.execute(text(sql), vals)
        return True

def detect_reserva_timestamp_cols(conn):
    created_candidates = ['creado_en', 'created_at', 'created_on', 'fecha_creacion']
    updated_candidates = ['actualizado_en', 'updated_at', 'updated_on', 'fecha_modificacion']
    rows = conn.execute(text(
        "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = 'reserva'"
    ), {"schema": DB_NAME}).fetchall()
    cols = {r[0] for r in rows}
    created = next((c for c in created_candidates if c in cols), None)
    updated = next((c for c in updated_candidates if c in cols), None)
    return created, updated

def load_optional_table(conn, table_name, df, fk_checks):
    """
    Inserta o actualiza filas de df en la tabla table_name verificando fk_checks dict:
    fk_checks: { 'col_name': 'referenced_table' }
    - Si la fila trae 'id' y ya existe: UPDATE SET <cols...> WHERE id = :id
    - Si la fila no trae 'id' o id no existe: INSERT
    Devuelve (inserted_count, updated_count, skipped_count, invalid_rows_df)
    """
    inserted = 0
    updated = 0
    skipped = 0
    invalid_rows = []

    # cache de ids existentes por tabla para evitar consultas repetidas
    fk_cache = {}
    def exists_in(table, id_):
        if id_ is None:
            return False
        if table not in fk_cache:
            rows = conn.execute(text(f"SELECT id FROM {table}")).fetchall()
            fk_cache[table] = {r[0] for r in rows}
        return id_ in fk_cache[table]

    # helper para comprobar si un id ya existe en la propia tabla destino
    def exists_in_dest(id_):
        if id_ is None:
            return False
        res = conn.execute(text(f"SELECT 1 FROM {table_name} WHERE id = :id LIMIT 1"), {"id": id_}).fetchone()
        return bool(res)

    for _, r in df.iterrows():
        row = r.to_dict()

        # normalizar fk ids y booleanos
        for fk_col, ref_table in fk_checks.items():
            if fk_col in row:
                row[fk_col] = _to_int_safe(row[fk_col])

        if 'valido' in row:
            b = _to_bool_safe(row['valido'])
            if b is not None:
                row['valido'] = b

        # Validar FKs: si alguna FK requerida no existe -> marcar inválida
        fk_ok = True
        for fk_col, ref_table in fk_checks.items():
            # si la columna no está en la fila la consideramos inválida
            if fk_col not in row or row.get(fk_col) is None:
                fk_ok = False
                break
            if not exists_in(ref_table, row.get(fk_col)):
                fk_ok = False
                break

        if not fk_ok:
            invalid_rows.append(row)
            continue

        try:
            # Si trae id y existe -> UPDATE
            id_val = _to_int_safe(row.get('id')) if 'id' in row else None
            if id_val is not None and exists_in_dest(id_val):
                # construir SQL de UPDATE con las columnas disponibles (excluir id)
                cols = [k for k in row.keys() if k != 'id' and row[k] is not None]
                if not cols:
                    skipped += 1
                    continue
                set_sql = ", ".join([f"{c} = :{c}" for c in cols])
                params = {c: row[c] for c in cols}
                params['id'] = id_val
                update_sql = f"UPDATE {table_name} SET {set_sql} WHERE id = :id"
                conn.execute(text(update_sql), params)
                updated += 1
            else:
                # insert (si trae id pero no existe, insertará con ese id)
                if insert_row_if_missing(conn, table_name, row, pk='id'):
                    inserted += 1
                else:
                    skipped += 1
        except Exception as e:
            log.warning("Error procesando tabla %s fila %s: %s", table_name, row, e)
            invalid_rows.append(row)

    invalid_df = pd.DataFrame(invalid_rows)
    return inserted, updated, skipped, invalid_df

def main():
    # Leer fuentes (incluye tablas opcionales)
    df_aerolinea = normalize_df(read_sheet_or_csv("aerolinea"))
    df_aeronave = normalize_df(read_sheet_or_csv("aeronave"))
    df_terminal = normalize_df(read_sheet_or_csv("terminal"))
    df_puerta = normalize_df(read_sheet_or_csv("puerta"))
    df_vuelo = normalize_df(read_sheet_or_csv("vuelo"))
    df_pasajero = normalize_df(read_sheet_or_csv("pasajero"))
    df_reserva = normalize_df(read_sheet_or_csv("reserva"))
    df_pase = normalize_df(read_sheet_or_csv("pase_abordar"))
    df_equipaje = normalize_df(read_sheet_or_csv("equipaje"))
    df_evento = normalize_df(read_sheet_or_csv("evento_embarque"))
    df_logs = normalize_df(read_sheet_or_csv("log_cambios"))

    # normalizar columna reserva (nombres)
    if not df_reserva.empty:
        df_reserva = df_reserva.rename(columns={
            'PNR': 'pnr', 'PasajeroID': 'pasajero_id', 'VueloID': 'vuelo_id',
            'Asiento': 'asiento', 'Estado': 'estado_reserva', 'FechaReserva': 'fecha_reserva'
        })
        for col in ['pnr', 'pasajero_id', 'vuelo_id']:
            if col not in df_reserva.columns:
                log.error("La fuente reserva no contiene la columna obligatoria '%s'. Abortando.", col)
                return

    # 1) Insertar maestros
    try:
        with engine.begin() as conn:
            summary = {"aerolinea": 0, "aeronave": 0, "terminal": 0, "puerta": 0, "vuelo": 0, "pasajero": 0}

            if not df_terminal.empty:
                for _, r in df_terminal.iterrows():
                    if insert_row_if_missing(conn, "terminal", r.to_dict(), pk='id'):
                        summary["terminal"] += 1

            if not df_aerolinea.empty:
                for _, r in df_aerolinea.iterrows():
                    if insert_row_if_missing(conn, "aerolinea", r.to_dict(), pk='id'):
                        summary["aerolinea"] += 1

            if not df_aeronave.empty:
                for _, r in df_aeronave.iterrows():
                    row = r.to_dict()
                    if 'capacidad' in row and 'capacidad_pasajeros' not in row:
                        row['capacidad_pasajeros'] = row.pop('capacidad')
                    if insert_row_if_missing(conn, "aeronave", row, pk='id'):
                        summary["aeronave"] += 1

            if not df_puerta.empty:
                for _, r in df_puerta.iterrows():
                    row = r.to_dict()
                    tid = _to_int_safe(row.get('terminal_id'))
                    if tid is not None:
                        exists = conn.execute(text("SELECT 1 FROM terminal WHERE id = :id LIMIT 1"), {"id": tid}).fetchone()
                        if not exists:
                            log.warning("terminal_id=%s de puerta %s no existe; se saltará.", tid, row.get('identificador'))
                            continue
                        row['terminal_id'] = tid
                    if insert_row_if_missing(conn, "puerta", row, pk='id'):
                        summary["puerta"] += 1

            if not df_vuelo.empty:
                for _, r in df_vuelo.iterrows():
                    row = r.to_dict()
                    aer_id = _to_int_safe(row.get('aerolinea_id'))
                    if aer_id is not None:
                        if not conn.execute(text("SELECT 1 FROM aerolinea WHERE id = :id LIMIT 1"), {"id": aer_id}).fetchone():
                            log.warning("aerolinea_id=%s para vuelo %s no existe; se saltará.", aer_id, row.get('numero_vuelo'))
                            continue
                        row['aerolinea_id'] = aer_id
                    aeronave_id = _to_int_safe(row.get('aeronave_id'))
                    if aeronave_id is not None:
                        if not conn.execute(text("SELECT 1 FROM aeronave WHERE id = :id LIMIT 1"), {"id": aeronave_id}).fetchone():
                            log.warning("aeronave_id=%s para vuelo %s no existe; se saltará.", aeronave_id, row.get('numero_vuelo'))
                            continue
                        row['aeronave_id'] = aeronave_id
                    puerta_id = _to_int_safe(row.get('puerta_id'))
                    if puerta_id is not None:
                        if not conn.execute(text("SELECT 1 FROM puerta WHERE id = :id LIMIT 1"), {"id": puerta_id}).fetchone():
                            log.warning("puerta_id=%s para vuelo %s no existe; se borrará referencia.", puerta_id, row.get('numero_vuelo'))
                            row['puerta_id'] = None
                        else:
                            row['puerta_id'] = puerta_id
                    if insert_row_if_missing(conn, "vuelo", row, pk='id'):
                        summary["vuelo"] += 1

            if not df_pasajero.empty:
                for _, r in df_pasajero.iterrows():
                    if insert_row_if_missing(conn, "pasajero", r.to_dict(), pk='id'):
                        summary["pasajero"] += 1

            log.info("Inserciones maestras realizadas: %s", summary)
    except SQLAlchemyError as e:
        log.exception("Error insertando maestros: %s", e)
        return

    # 2) Merge reservas (staging)
    if df_reserva.empty:
        log.info("No hay datos de reservas en la fuente. Fin del proceso.")
    else:
        try:
            df_reserva['pasajero_id'] = df_reserva['pasajero_id'].apply(_to_int_safe)
            df_reserva['vuelo_id'] = df_reserva['vuelo_id'].apply(_to_int_safe)
        except Exception:
            log.warning("No se pudo castear IDs a int en reservas; revisa los datos.")

        df_reserva = df_reserva.drop_duplicates(subset=['pnr', 'vuelo_id'])

        with engine.connect() as conn_check:
            pasajeros_db = {r[0] for r in conn_check.execute(text("SELECT id FROM pasajero")).fetchall()}
            vuelos_db = {r[0] for r in conn_check.execute(text("SELECT id FROM vuelo")).fetchall()}

        df_valid = df_reserva[df_reserva['pasajero_id'].isin(pasajeros_db) & df_reserva['vuelo_id'].isin(vuelos_db)].copy()
        df_invalid = df_reserva[~df_reserva.index.isin(df_valid.index)]
        if not df_invalid.empty:
            os.makedirs(DATA_DIR, exist_ok=True)
            out = os.path.join(DATA_DIR, "reserva_invalidas_post_maestros.csv")
            df_invalid.to_csv(out, index=False, encoding='utf-8')
            log.warning("%d filas de reservas inválidas por FK guardadas en %s", len(df_invalid), out)

        if not df_valid.empty:
            ts = int(time.time())
            staging_table = f"reserva_staging_{os.getpid()}_{ts}"
            try:
                df_valid.to_sql(staging_table, con=engine, if_exists='replace', index=False)
                log.info("Staging creado fuera de la transacción: %s (%d filas)", staging_table, len(df_valid))
            except Exception as e:
                log.exception("Error creando staging: %s", e)
                return

            try:
                with engine.begin() as conn:
                    created_col, updated_col = detect_reserva_timestamp_cols(conn)
                    log.info("Columnas timestamp detectadas en 'reserva': creado='%s', actualizado='%s'", created_col, updated_col)

                    insert_select_cols = ["rs.pnr", "rs.pasajero_id", "rs.vuelo_id", "rs.asiento", "rs.estado_reserva", "rs.fecha_reserva"]
                    insert_cols_names = ["pnr", "pasajero_id", "vuelo_id", "asiento", "estado_reserva", "fecha_reserva"]
                    if created_col:
                        insert_select_cols.append("NOW()")
                        insert_cols_names.append(created_col)
                    if updated_col:
                        insert_select_cols.append("NOW()")
                        insert_cols_names.append(updated_col)

                    insert_select_sql = ", ".join(insert_select_cols)
                    insert_cols_sql = ", ".join(insert_cols_names)

                    insert_sql = f"""
                    INSERT INTO reserva ({insert_cols_sql})
                    SELECT {insert_select_sql}
                    FROM {staging_table} rs
                    LEFT JOIN reserva r ON r.pnr = rs.pnr AND r.vuelo_id = rs.vuelo_id
                    WHERE r.id IS NULL;
                    """

                    update_set = [
                        "r.asiento = rs.asiento",
                        "r.estado_reserva = rs.estado_reserva",
                        "r.fecha_reserva = rs.fecha_reserva"
                    ]
                    if updated_col:
                        update_set.append(f"r.{updated_col} = NOW()")
                    update_set_sql = ",\n                    ".join(update_set)

                    update_sql = f"""
                    UPDATE reserva r
                    JOIN {staging_table} rs ON r.pnr = rs.pnr AND r.vuelo_id = rs.vuelo_id
                    SET {update_set_sql};
                    """

                    res_ins = conn.execute(text(insert_sql))
                    res_upd = conn.execute(text(update_sql))

                    try:
                        inserted = res_ins.rowcount if res_ins.rowcount is not None and res_ins.rowcount >= 0 else None
                    except Exception:
                        inserted = None
                    try:
                        updated = res_upd.rowcount if res_upd.rowcount is not None and res_upd.rowcount >= 0 else None
                    except Exception:
                        updated = None

                    log.info("Reservas: insertadas (aprox) = %s, actualizadas (aprox) = %s", inserted, updated)

            except SQLAlchemyError as e:
                log.exception("Error en operaciones de base de datos durante merge: %s", e)
            finally:
                try:
                    with engine.connect() as conn_drop:
                        conn_drop.execute(text(f"DROP TABLE IF EXISTS {staging_table}"))
                        log.info("Tabla staging eliminada: %s", staging_table)
                except Exception as e:
                    log.warning("No se pudo eliminar la staging %s: %s", staging_table, e)

    # 3) Importar tablas opcionales (pase_abordar -> equipaje -> evento_embarque -> log_cambios)
    try:
        with engine.begin() as conn:
            # pase_abordar necesita reserva_id existente
            if not df_pase.empty:
                inserted, updated, skipped, invalid_df = load_optional_table(conn, "pase_abordar", df_pase, {"reserva_id": "reserva"})
                log.info("pase_abordar: insertadas=%d, actualizadas=%d, saltadas=%d, invalidas=%d", inserted, updated, skipped, 0 if invalid_df.empty else len(invalid_df))
                if not invalid_df.empty:
                    invalid_df.to_csv(os.path.join(DATA_DIR, "pase_abordar_invalidas.csv"), index=False, encoding='utf-8')

            # equipaje necesita reserva_id y vuelo_id
            if not df_equipaje.empty:
                inserted, updated, skipped, invalid_df = load_optional_table(conn, "equipaje", df_equipaje, {"reserva_id": "reserva", "vuelo_id": "vuelo"})
                log.info("equipaje: insertadas=%d, actualizadas=%d, saltadas=%d, invalidas=%d", inserted, updated, skipped, 0 if invalid_df.empty else len(invalid_df))
                if not invalid_df.empty:
                    invalid_df.to_csv(os.path.join(DATA_DIR, "equipaje_invalidas.csv"), index=False, encoding='utf-8')

            # evento_embarque necesita pase_abordar_id y puerta_id (usuario_id map to aerolinea.id for tests)
            if not df_evento.empty:
                inserted, updated, skipped, invalid_df = load_optional_table(conn, "evento_embarque", df_evento, {"pase_abordar_id": "pase_abordar", "puerta_id": "puerta", "usuario_id": "aerolinea"})
                log.info("evento_embarque: insertadas=%d, actualizadas=%d, saltadas=%d, invalidas=%d", inserted, updated, skipped, 0 if invalid_df.empty else len(invalid_df))
                if not invalid_df.empty:
                    invalid_df.to_csv(os.path.join(DATA_DIR, "evento_embarque_invalidas.csv"), index=False, encoding='utf-8')

            # log_cambios: no requiere FK estricto (solo registro)
            if not df_logs.empty:
                inserted = 0
                skipped = 0
                invalid_rows = []
                for _, r in df_logs.iterrows():
                    row = r.to_dict()
                    # si viene id convertir a int
                    try:
                        if 'id' in row:
                            row['id'] = _to_int_safe(row['id'])
                    except Exception:
                        pass
                    try:
                        if insert_row_if_missing(conn, "log_cambios", row, pk='id'):
                            inserted += 1
                        else:
                            skipped += 1
                    except Exception as e:
                        log.warning("Error insertando log_cambios fila %s: %s", row, e)
                        invalid_rows.append(row)
                log.info("log_cambios: insertadas=%d, saltadas=%d, invalidas=%d", inserted, skipped, len(invalid_rows))
                if invalid_rows:
                    pd.DataFrame(invalid_rows).to_csv(os.path.join(DATA_DIR, "log_cambios_invalidas.csv"), index=False, encoding='utf-8')

    except SQLAlchemyError as e:
        log.exception("Error importando tablas opcionales: %s", e)
    except Exception as e:
        log.exception("Error inesperado importando opcionales: %s", e)

if __name__ == "__main__":
    main()