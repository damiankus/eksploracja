#!/usr/bin/env python3

from glob import glob
import argparse
import os
import psycopg2


def fetch_inventor_data(root_path, connection, sep=',', psep=os.path.sep):
    cursor = connection.cursor()

    statement = 'SELECT name AS "Imię i nazwisko", \
        country AS "Kraj", state AS "Stan", city AS "Miasto", \
        company AS "Firmy", subcat AS "Branże" \
        FROM get_inventor_data(%s) ORDER BY company, subcat'
    target_root_dir = 'metrics-info'

    try:
        for csv_path in glob(os.path.join(root_path, '*.csv')):
            path_levels = csv_path.split(psep)
            target_dir = psep.join([target_root_dir] + path_levels[1:-1])
            target_path = psep.join([target_dir, path_levels[-1]])

            with open(csv_path, 'r') as csv:
                if not os.path.isdir(target_dir):
                    os.makedirs(target_dir)

                with open(target_path, 'w+') as target:
                    # skip the patent id colmn
                    colnames = csv.readline().strip().split(sep)
                    qcolnames = []
                    first_query = True

                    for line in csv:
                        metrics_row = line.strip().split(sep)
                        print('Inventor ID: {}'.format(metrics_row[0]))

                        # The first column contains patent ID
                        cursor.execute(statement, [metrics_row[0]])
                        connection.commit()
                        rows = cursor.fetchall()

                        if first_query:
                            qcolnames = [desc[0] for desc in cursor.description]
                            colnames.extend(qcolnames)
                            first_query = False
                            target.write(','.join(colnames) + '\r\n')

                        common_cols = [str(col).strip() for col in rows[0][:4]]
                        companies = [', '.join(set([str(row[4]).strip() for row in rows]))]
                        branches = [', '.join(set([str(row[5]).strip() for row in rows]))]
                        csv_row = metrics_row + common_cols + companies + branches
                        csv_row = sep.join(['"' + str(e) + '"' for e in csv_row])
                        target.write(csv_row + '\r\n')

    except Exception as e:
        connection.rollback()
        print(e)
        raise e


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch inventor data based on \
        the content of CSV files')
    parser.add_argument('--root', '-r', help='Path of the root directory \
        to traverse', required=True)
    args = parser.parse_args()

    db_config = {
        'dbname': 'patents',
        'user': 'damian',
        'password': 'pass'
    }

    try:
        connection = psycopg2.connect(**db_config)
        fetch_inventor_data(args.root, connection)
    except Exception as e:
        if (connection):
            connection.close()
        raise e
