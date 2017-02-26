import os
import sys
import threading

import psycopg2
import queue
import random
import string
import time


class EE382V_DB_EGR:
    def __init__(self, db_conf, variation=1, num_row=5000000, bulk_row=0):
        self.db_conf = db_conf
        self.variation = variation
        self.insert_statement = 'INSERT INTO benchmark VALUES'
        self.data_queue = queue.Queue()
        self.rand_generator = random.Random(x=0)
        self.num_row = num_row
        self.bulk_row = bulk_row
        self.generate_data_time = 0
        self.insert_run_time = 0
        self.__key_indexes = list(range(1, self.num_row + 1))
        self.__rand_str = []
        for i in range(30):
            filter_length = self.rand_generator.randint(1, 247)
            filter_str = EE382V_DB_EGR.rand_str(filter_length)
            self.__rand_str.append(filter_str)

        return

    def run_load_data(self):
        if self.variation == 2:
            random.shuffle(self.__key_indexes)

        self.__clean_db_table()
        self.__create_index()
        print('Run lab 1 - variation {} with {} records'.format(self.variation, self.num_row))
        self.__clean_db_table()
        start_time = time.clock()
        self.__run_set_up()
        total_time = time.clock() - start_time
        print('Total time for variation  {} - {} records: {}'.format(self.variation, self.num_row, total_time))
        print('Total time for generate data{} records: {}'.format(self.num_row, self.generate_data_time))
        print('Total time for insertion data - {} records: {}'.format(self.num_row,
                                                                      self.insert_run_time))
        with open('log_perf.csv', 'a') as file:
            file.write('{}|{}|{}|{}|{}|{}\n'.format(self.variation, self.num_row, self.bulk_row, total_time,
                                                    self.generate_data_time, self.insert_run_time))

        return

    def __run_set_up(self):
        #read_thread = threading.Thread(target=self.__generate_data)
        #read_thread.start()
        #process_thread = threading.Thread(target=self.__process_data)
        #process_thread.start()
        #read_thread.join()
        #process_thread.join()
        self.__generate_data()
        self.__process_data()
        return

    def __generate_data(self):
        head = 1
        tail = 50000

        start_time = time.clock()
        for i in range(1, self.num_row + 1):
            the_key = self.__generate_the_key(i)
            column_a = self.rand_generator.randint(head, tail)
            column_b = self.rand_generator.randint(head, tail)
            filter_str = self.__rand_str[i % len(self.__rand_str)]
            self.data_queue.put((the_key, column_a, column_b, filter_str))

        self.generate_data_time = time.clock() - start_time
        return

    def __process_data(self):
        row_counter = 0
        records = []
        start_time = time.clock()
        while row_counter < self.num_row:
            records.append(self.data_queue.get())
            row_counter += 1
            if self.bulk_row == 0 or len(records) % self.bulk_row == 0:
                self.__insert_data(self.insert_statement, records)
                records = []

        self.insert_run_time = time.clock() - start_time
        return

    def __insert_data(self, ins_stm, records):
        insert_query = '{} {}'.format(ins_stm, ','.join(['%s'] * len(records)))
        with psycopg2.connect(self.db_conf) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(insert_query, records)

        return

    def __generate_the_key(self, i):
        if self.variation == 1:
            return i
        else:
            return self.__key_indexes.pop()

    def __clean_db_table(self):
        trunc_stm = 'TRUNCATE TABLE benchmark'
        with psycopg2.connect(self.db_conf) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(trunc_stm)

        return

    def __create_index(self, columns=[]):
        if len(columns) == 1:
            sql_stm = 'CREATE INDEX IDX_LAB ON benchmark("{}")'.format(columns[0])
        elif len(columns) == 2:
            sql_stm = 'CREATE INDEX IDX_LAB ON benchmark("{}", "{}")'.format(columns[0], columns[1])

        with psycopg2.connect(self.db_conf) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            try:
                cursor.execute('DROP INDEX IDX_LAB')
            except Exception as e:
                print("Error occur while droping index: {}".format(str(e)))

            if len(columns) > 0:
                try:
                    cursor.execute(sql_stm)
                except Exception as e:
                    print("Error occur while creating index of {} : {}".format(columns, str(e)))

        return

    def bench_mark(self, ver):
        q1_perf = []
        q2_perf = []
        q3_perf = []
        column_a = [25248, 27563, 16969, 31846, 19878, 23466, 14316, 9128, 6215, 16418, 46215, 9632, 6473, 4833, 21640,
                    36688, 23187, 20723, 41971, 40036]
        column_b = [49674, 2654, 33507, 26538, 31235, 38233, 33076, 18471, 49533, 40526, 34903, 39477, 20326, 47831,
                    44826, 30943, 6600, 28454, 40036, 13401]
        table_name = 'benchmark'
        with psycopg2.connect(self.db_conf) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            for index, item in enumerate(zip(column_a, column_b)):
                start_t = time.clock()
                cursor.execute('SELECT * FROM {0} WHERE {0}."columnA"=%s'.format(table_name), (item[0],))
                q1_perf.append(time.clock() - start_t)

                start_t = time.clock()
                cursor.execute('SELECT * FROM {0} WHERE {0}."columnB"=%s'.format(table_name), (item[1],))
                q2_perf.append(time.clock() - start_t)

                start_t = time.clock()
                cursor.execute('SELECT * FROM {0} WHERE {0}."columnA"=%s AND {0}."columnB"=%s'.format(table_name),
                               (item[0], item[1]))
                q3_perf.append(time.clock() - start_t)

            with open('q_log_perf.csv', 'a') as file:
                file.write('{}|{}|{}|{}|{}|{}\n'.format(self.variation, ver, len(column_a),
                                                        sum(q1_perf, 0.0) / len(q1_perf),
                                                        sum(q2_perf, 0.0) / len(q2_perf),
                                                        sum(q3_perf, 0.0) / len(q3_perf)))

        return

    def run_benchmark_v1(self):
        self.__create_index()
        self.bench_mark("ver1")
        return

    def run_benchmark_v2(self):
        self.__create_index(["columnA"])
        self.bench_mark("ver2")
        return

    def run_benchmark_v3(self):
        self.__create_index(["columnB"])
        self.bench_mark("ver3")
        return

    def run_benchmark_v4(self):
        self.__create_index(["columnA", "columnB"])
        self.bench_mark("ver4")
        return

    def run_lab(self, load_data=False):
        if load_data:
            self.run_load_data()

        self.run_benchmark_v1()
        self.run_benchmark_v2()
        self.run_benchmark_v3()
        self.run_benchmark_v4()
        return

    @staticmethod
    def rand_str(length):
        return ''.join(
            random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for _ in range(length))


if __name__ == '__main__':
    connect_str = "dbname='ee382v-db-engr' user='python' host='localhost' password='python'"
    argv = sys.argv
    n_rows = int(argv[1])
    variation_1 = EE382V_DB_EGR(connect_str, variation=1, num_row=n_rows, bulk_row=1000)
    variation_1.run_lab(load_data=True)
    variation_2 = EE382V_DB_EGR(connect_str, variation=2, num_row=n_rows, bulk_row=1000)
    variation_2.run_lab(load_data=True)
