from mysql.connector import MySQLConnection, Error
from python_mysql_dbconfig import read_db_config
import pandas as pd


def query_with_fetchall():
    try:
        dbconfig = read_db_config()
        conn = MySQLConnection(**dbconfig)
        cursor = conn.cursor()
        cursor.execute("""
                        SELECT
                         l.cod_loja, l.coddig_loja, l.nome_loja, l.apelido_loja, r.user_rms, 
                         r.pass_rms, r.names_rms, w.user_web, w.pass_web, w.names_web 
                        from loja as l, rms as r, syspdv as s, web as w where 1=1
                         and l.cod_rms = r.cod_rms 
                         and l.cod_syspdv = s.cod_syspdv
                         and l.cod_web = w.cod_web 
                         and l.cv = 1 
                         order by l.ord_cap_int asc""")
        rows = cursor.fetchall()
        n_rows = cursor.rowcount
        print('Total Row(s):', n_rows)
        columns = ['cod_loja', 'coddig_loja', 'nome_loja', 'apelido_loja', 'user_rms',
                   'pass_rms', 'names_rms', 'user_web', 'pass_web', 'names_web']

        selected_rows = []
        for row in rows:
            selected_rows.append(row[:len(columns)])

        df = pd.DataFrame(selected_rows, columns=columns)


        return df
    except Error as e:
        print(e)

    finally:
        cursor.close()
        conn.close()


if __name__ == '__main__':
    query_with_fetchall()