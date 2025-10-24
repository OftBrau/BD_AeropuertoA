

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