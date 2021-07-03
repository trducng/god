"""Parse logic update into SQL statements"""


def parse_to_db_statements(logic, record_entries, primary_cols):
    """Parse the logic into SQL statements

    # Args:
        logic <{id: [col: {('+/-', value)]}}>: the logic
        record_entries <{id: {cols: values}}>: entries in current record
        primary_cols <[str]>: primary column name

    # Returns:
        <[str]>: list of sql statements from logic
    """
    sql_statements = []
    for fid, cols in logic.items():
        if fid in record_entries:
            sql_statement = []
            drop = False
            for col_name, changes in cols.items():
                op, value = changes[-1]
                if op == "+" and value != record_entries[fid][col_name]:
                    sql_statement.append(f'{col_name} = "{value}"')
                elif op == "-":
                    if col_name in primary_cols:
                        drop = True
                    sql_statement.append(f"{col_name} = NULL")
            if drop:
                sql_statements.append(f'DELETE FROM main WHERE id="{fid}"')
                continue

            if sql_statement:
                sql_statements.append(
                    f"UPDATE main SET {', '.join(sql_statement)} WHERE id = \"{fid}\""
                )
        else:
            add_col, add_val = [], []
            for col_name, changes in cols.items():
                op, value = changes[-1]
                if op == "+":
                    add_col.append(col_name)
                    add_val.append(f"{value}")

            if add_col:
                add_col = ["id"] + add_col
                add_val = [fid] + add_val
                sql_statements.append(
                    f"INSERT INTO main {tuple(add_col)} VALUES {tuple(add_val)}"
                )

    return sql_statements
