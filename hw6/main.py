import os
import sys
import threading

import psycopg2
import queue
import random
import string
import time


class EE382V_DB_EGR:
    def __init__(self, db_conf, num_row=5000000, bulk_row=0):
        self.db_conf = db_conf
        self.insert_statement = 'INSERT INTO benchmark VALUES'
        self.data_queue = queue.Queue()
        self.rand_generator = random.Random(x=0)
        self.num_row = num_row
        self.bulk_row = bulk_row
        self.generate_data_time = 0
        self.insert_run_time = 0
        self.__rand_str = []
        for i in range(30):
            filter_length = self.rand_generator.randint(1, 247)
            filter_str = EE382V_DB_EGR.rand_str(filter_length)
            self.__rand_str.append(filter_str)
        return

    def run_load_data(self, ver=1):
        if ver == 2:
            self.__key_indexes = list(range(1, self.num_row + 1))
            random.shuffle(self.__key_indexes)
        self.__clean_db_table()
        print('Run lab 1 - variation {} with {} records'.format(ver, self.num_row))
        self.__clean_db_table()
        start_time = time.clock()
        self.__run_set_up(ver)
        total_time = time.clock() - start_time
        print('Total time for variation  {} - {} records: {}'.format(ver, self.num_row, total_time))
        print('Total time for generate data{} records: {}'.format(self.num_row, self.generate_data_time))
        print('Total time for insertion data - {} records: {}'.format(self.num_row,
                                                                                     self.insert_run_time))
        with open('log_perf.csv', 'a') as file:
            file.write('{}|{}|{}|{}|{}|{}\n'.format(ver, self.num_row, self.bulk_row, total_time, self.generate_data_time, self.insert_run_time))


        return

    def __run_set_up(self, ver=1):
        read_thread = threading.Thread(target=self.__generate_data, args=(ver,))
        read_thread.start()
        process_thread = threading.Thread(target=self.__process_data, args=(ver,))
        process_thread.start()
        read_thread.join()
        process_thread.join()
        #self.__generate_data(ver)
        #self.__process_data(ver)
        return

    def __generate_data(self, ver=1):
        head = 1
        tail = 50000

        start_time = time.clock()
        for i in range(1, self.num_row + 1):
            the_key = self.__generate_the_key(ver, i)
            column_a = self.rand_generator.randint(head, tail)
            column_b = self.rand_generator.randint(head, tail)
            filter_str = self.__rand_str[i % len(self.__rand_str)]
            self.data_queue.put((the_key, column_a, column_b, filter_str))

        self.generate_data_time = time.clock() - start_time

        return

    def __process_data(self, ver=1):
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

    def __generate_the_key(self, ver, i):
        if ver==1:
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

    def bench_mark(self, variation=1, ver="ver1", n_run=10):
        column_a = []
        column_b = []
        q1_perf = []
        q2_perf = []
        q3_perf = []
        #for i in range(0, n_run):
        #    column_a.append(self.rand_generator.randint(1, 50000))
        #    column_b.append(self.rand_generator.randint(1, 50000))

        column_a = [25248, 27563, 16969, 31846, 19878, 23466, 14316, 9128, 6215, 16418, 46215, 9632, 6473, 4833, 21640, 36688, 23187, 20723, 41971, 40036]
        column_b = [49674, 2654, 33507, 26538, 31235, 38233, 33076, 18471, 49533, 40526, 34903, 39477, 20326, 47831, 44826, 30943, 6600, 28454, 40036, 13401]
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
                file.write('{}|{}|{}|{}|{}|{}\n'.format(variation, ver, n_run,
                                                   sum(q1_perf, 0.0) / len(q1_perf), sum(q2_perf, 0.0) / len(q2_perf),
                                                   sum(q3_perf, 0.0) / len(q3_perf)))

        return

    def run_lab(self, ver=1, load_data= False, phys_ver="ver1", n_rows=5000000, n_run= 10):
        if load_data:
            self.run_load_data(ver)

        self.bench_mark(ver, phys_ver, n_run)



    @staticmethod
    def rand_str(length):
        return ''.join(
            random.choice(string.ascii_lowercase + string.ascii_uppercase + string.digits) for _ in range(length))


if __name__ == '__main__':
    connect_str = "dbname='ee382v-db-engr' user='python' host='localhost' password='python'"
    #obj.run_load_data()
    argv = sys.argv
    ver = int(argv[1])
    load_data = argv[2] == "1"
    phys_ver = argv[3]
    n_rows = int(argv[4])
    n_run = argv[5]
    obj = EE382V_DB_EGR(connect_str, num_row=n_rows, bulk_row=1000)
    obj.run_lab(ver, load_data, phys_ver, n_run)



