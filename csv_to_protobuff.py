import bottleneck_pb2
import csv
import os
import pandas as pd

class CSV_to_Proto:
    def __init__(self, serialize_file="0"):
        self.serialize_file = serialize_file
        self.binary_dir = "./binary_logs"
        self.bottleneck_logs = self.check_serialize_file(self.serialize_file)
        self.id_to_attr = {1: 'avg_rtt_value', 2: 'pacing_avg_rate', 3: 'avg_cwnd_rate', 4: 'avg_retransmission_timeout_value', 5: 'avg_byte_ack', 6: 'avg_seg_out', 7: 'retrans', 8: 'avg_mss_rate', 9: 'avg_ssthresh_rate', 10: 'avg_seg_in', 11: 'avg_send_value', 12: 'avg_unacked_value', 13: 'avg_rcv_space_rate', 14: 'send_buffer_value', 15: 'read_req', 16: 'write_req', 17: 'rkB', 18: 'wkB', 19: 'rrqm', 20: 'wrqm', 21: 'rrqm_perc', 22: 'wrqm_perc', 23: 'r_await', 24: 'w_await', 25: 'aqu_sz', 26: 'rareq_sz', 27: 'wareq_sz', 28: 'svctm', 29: 'util', 30: 'rchar', 31: 'wchar', 32: 'syscr', 33: 'syscw', 34: 'read_bytes', 35: 'write_bytes', 36: 'cancelled_write_bytes', 37: 'pid', 38: 'ppid', 39: 'pgrp', 40: 'session', 41: 'tty_nr', 42: 'tpgid', 43: 'flags', 44: 'minflt', 45: 'cminflt', 46: 'majflt', 47: 'cmajflt', 48: 'utime', 49: 'stime', 50: 'cutime', 51: 'cstime', 52: 'priority', 53: 'nice', 54: 'num_threads', 55: 'itrealvalue', 56: 'starttime', 57: 'vsize', 58: 'rss', 59: 'rsslim', 60: 'startcode', 61: 'endcode', 62: 'startstack', 63: 'kstkesp', 64: 'kstkeip', 65: 'signal', 66: 'blocked', 67: 'sigignore', 68: 'sigcatch', 69: 'wchan', 70: 'nswap', 71: 'cnswap', 72: 'exit_signal', 73: 'processor', 74: 'rt_priority', 75: 'policy', 76: 'delayacct_blkio_ticks', 77: 'guest_time', 78: 'cguest_time', 79: 'start_data', 80: 'end_data', 81: 'start_brk', 82: 'arg_start', 83: 'arg_end', 84: 'env_start', 85: 'env_end', 86: 'exit_code', 87: 'cpu_usage_percentage', 88: 'mem_usage_percentage', 89: 'tcp_rcv_buffer_min', 90: 'tcp_rcv_buffer_default', 91: 'tcp_rcv_buffer_max', 92: 'tcp_snd_buffer_min', 93: 'tcp_snd_buffer_default', 94: 'tcp_snd_buffer_max', 95: 'label_value'}
        self.datatypes = {  1: 'float', 2: 'string', 3: 'float', 4: 'float', 5: 'float', 6: 'float', 7: 'float', 8: 'float', 9: 'float', 10: 'float', 11: 'string', 12: 'float', 13: 'float', 14: 'float', 15: 'float', 16: 'float', 17: 'float', 18: 'float', 19: 'float', 20: 'float', 21: 'float', 22: 'float', 23: 'float', 24: 'float', 25: 'float', 26: 'float', 27: 'float', 28: 'float', 29: 'float', 30: 'int', 31: 'int', 32: 'int', 33: 'int', 34: 'int', 35: 'int', 36: 'int', 37: 'int', 38: 'int', 39: 'int', 40: 'int', 41: 'int', 42: 'int', 43: 'int', 44: 'int', 45: 'int', 46: 'int', 47: 'int', 48: 'int', 49: 'int', 50: 'int', 51: 'int', 52: 'int', 53: 'int', 54: 'int', 55: 'int', 56: 'int', 57: 'int', 58: 'int', 
                            59: 'intonly', 60: 'intonly', 61: 'intonly', 62: 'intonly', 63: 'int', 64: 'int', 65: 'int', 66: 'int', 67: 'int', 68: 'int', 69: 'int', 70: 'int', 71: 'int', 72: 'int', 73: 'int', 74: 'int', 75: 'int', 76: 'int', 77: 'int', 78: 'int', 79: 'int', 80: 'int', 81: 'int', 82: 'int', 83: 'int', 84: 'int', 85: 'int', 86: 'int', 87: 'float', 88: 'float', 89: 'int', 90: 'int', 91: 'int', 92: 'int', 93: 'int', 94: 'int', 95: 'int'}
    def read_csv(self, filename):
        data = []
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            for row in csv_reader:
                new_row = []
                for i in range(len(row)):
                    type_ = self.datatypes[i+1]
                    if type_ == "string":
                        new_row.append(row[i])
                    elif type_ == "float":
                        new_row.append(float(row[i]))
                    elif type_ == "intonly":
                        new_row.append(int(row[i]))
                    else:
                        new_row.append(int(float(row[i])))
                data.append(new_row)
        return data

    def write_to_proto(self, filename):
        logs = self.read_csv(filename)
        this_file_logs = bottleneck_pb2.BottleneckLogs()
        for log in logs:
            new_log = bottleneck_pb2.BottleneckLog()
            for i in range(len(log)):
                new_log.__setattr__(self.id_to_attr[i+1], log[i])
            this_file_logs.logs.append(new_log)
        self.bottleneck_logs.logs.extend(this_file_logs.logs)
            
    def write_to_binary(self, serialize_file):
        try:
            file_path = self.binary_dir+"/"+serialize_file
            f = open(file_path, "wb")
            f.write(self.bottleneck_logs.SerializeToString())
            f.close()
            print("Binary file of path: " + file_path + " is created.")
        except IOError:
            print("[+] Error while creating binary file.")

    def add_all_dataset_files(self, folder):
        files = os.listdir(folder)
        for filename in files:
            if "dataset" in filename and "csv" in filename:
                self.write_to_proto(folder+"/"+filename)

    def check_serialize_file(self, serialize_file):
        full_path = self.binary_dir + "/" + serialize_file
        bottleneck_logs = bottleneck_pb2.BottleneckLogs()
        if os.path.isfile(full_path):
            try:
                f = open(full_path, "rb")
                string_ = f.read()
                bottleneck_logs.ParseFromString(string_)
                f.close()
            except IOError:
                print(full_path + ": Could not open file.  Creating a new one.")
        return bottleneck_logs

serialize_file = "0"
csv_to_python = CSV_to_Proto(serialize_file)
csv_to_python.add_all_dataset_files("./csv_logs")
csv_to_python.write_to_binary(csv_to_python.serialize_file)
