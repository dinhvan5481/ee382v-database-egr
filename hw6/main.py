import os
import threading

import psycopg2
import queue
import random
import string
import time


class EE382V_DB_EGR:
    def __init__(self, db_conf, num_row=5000000, bulk_row=0):
        self.db_conf = db_conf
        self.insert_statement_v1 = 'INSERT INTO benchmark_v1 VALUES'
        self.insert_statement_v2 = 'INSERT INTO benchmark_v2 VALUES'
        self.data_queue = queue.Queue()
        self.rand_generator = random.Random(x=0)
        self.num_row = num_row
        self.bulk_row = bulk_row
        self.generate_data_time_v1 = 0
        self.generate_data_time_v2 = 0
        self.insert_run_time_v1 = 0
        self.insert_run_time_v2 = 0
        self.__key_indexes = list(range(1, self.num_row + 1))
        random.shuffle(self.__key_indexes)
        return

    def run(self):
        print('Run lab 1 with {} records'.format(self.num_row))
        self.__clean_db_table(in_order=True)
        start_time = time.clock()
        self.__run_set_up(in_order=True)
        total_time_v1 = time.clock() - start_time
        print('Total time for variation 1 - {} records: {}'.format(self.num_row, total_time_v1))
        print('Total time for generate data for variation 1 - {} records: {}'.format(self.num_row, self.generate_data_time_v1))
        print('Total time for insertion data for variation 1 - {} records: {}'.format(self.num_row,
                                                                                     self.insert_run_time_v1))

        self.__clean_db_table(in_order=False)
        start_time = time.clock()
        self.__run_set_up(in_order=False)
        total_time_v2 = time.clock() - start_time
        print('Total time for variation 2 - {} records: {}'.format(self.num_row, total_time_v2))
        print('Total time for generate data for variation 2 - {} records: {}'.format(self.num_row, self.generate_data_time_v2))
        print('Total time for insertion data for variation 2 - {} records: {}'.format(self.num_row,
                                                                                     self.insert_run_time_v2))

        with open('log_perf.csv', 'a') as file:
            file.write('1|{}|{}|{}|{}|{}\n'.format(self.num_row, self.bulk_row, total_time_v1, self.generate_data_time_v1, self.insert_run_time_v1))
            file.write('2|{}|{}|{}|{}|{}\n'.format(self.num_row, self.bulk_row, total_time_v2, self.generate_data_time_v2, self.insert_run_time_v2))


        return

    def __run_set_up(self, in_order=True):
        read_thread = threading.Thread(target=self.__generate_data, args=(in_order,))
        read_thread.start()
        process_thread = threading.Thread(target=self.__process_data, args=(in_order,))
        process_thread.start()
        read_thread.join()
        process_thread.join()
        # self.__generate_data(in_order)
        # self.__process_data(in_order)
        return

    def __generate_data(self, in_order=True):
        head = 1
        tail = 50000
        if not in_order:
            self.__key_indexes = list(range(1, self.num_row + 1))
            random.shuffle(self.__key_indexes)

        start_time = time.clock()
        for i in range(1, self.num_row + 1):
            the_key = self.__generate_the_key(in_order, i)
            column_a = self.rand_generator.randint(head, tail)
            column_b = self.rand_generator.randint(head, tail)
            filter_length = self.rand_generator.randint(1, 247)
            filter_str = EE382V_DB_EGR.rand_str(filter_length)
            self.data_queue.put((the_key, column_a, column_b, filter_str))

        if in_order:
            self.generate_data_time_v1 = time.clock() - start_time
        else:
            self.generate_data_time_v2 = time.clock() - start_time

        return

    def __process_data(self, in_order=True):
        row_counter = 0
        records = []

        start_time = time.clock()
        while row_counter < self.num_row:
            records.append(self.data_queue.get())
            row_counter += 1
            if self.bulk_row == 0 or len(records) % self.bulk_row == 0:
                if in_order:
                    self.__insert_data(self.insert_statement_v1, records)
                else:
                    self.__insert_data(self.insert_statement_v2, records)
                records = []

        if in_order:
            self.insert_run_time_v1 = time.clock() - start_time
        else:
            self.insert_run_time_v2 = time.clock() - start_time

        return

    def __insert_data(self, ins_stm, records):
        insert_query = '{} {}'.format(ins_stm, ','.join(['%s'] * len(records)))
        with psycopg2.connect(self.db_conf) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(insert_query, records)

        return

    def __generate_the_key(self, in_order, i):
        if in_order:
            return i
        else:
            return self.__key_indexes.pop()

    def __clean_db_table(self, in_order):
        if in_order:
            trunc_stm = 'TRUNCATE TABLE benchmark_v1'
        else:
            trunc_stm = 'TRUNCATE TABLE benchmark_v2'

        with psycopg2.connect(self.db_conf) as conn:
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(trunc_stm)
        return

    def bench_mark(self, n_run=10, ver="no_index"):
        column_a = []
        column_b = []
        q1_perf = []
        q2_perf = []
        q3_perf = []
        for i in range(0, n_run):
            column_a.append(self.rand_generator.randint(1, 50000))
            column_b.append(self.rand_generator.randint(1, 50000))

        q1 = 'SELECT * FROM %s WHERE columnA=%s'
        q2 = 'SELECT * FROM %s WHERE columnB=%s'
        q3 = 'SELECT * FROM %s WHERE columnA=%s and columnB=%s'

        for i in [1, 2]:
            table_name = 'benchmark_v{}'.format(i)

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
                file.write('{}|{}|{}|{}|{}|{}\n'.format(table_name, ver, n_run,
                                                   sum(q1_perf, 0.0) / len(q1_perf), sum(q2_perf, 0.0) / len(q2_perf),
                                                   sum(q3_perf, 0.0) / len(q3_perf)))

        return

    @staticmethod
    def rand_str(length):
        return ''.join(
            random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for _ in range(length))


if __name__ == '__main__':
    connect_str = "dbname='ee382v-db-engr' user='python' host='localhost' password='python'"
    obj = EE382V_DB_EGR(connect_str, num_row=1, bulk_row=1000)
    ver = 'ver4'
    obj.bench_mark(n_run=20, ver=ver)



