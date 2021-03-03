import bottleneck_pb2
import csv
import os
import pandas as pd
import re

class CSV_to_Proto:
    def __init__(self, serialize_file="0"):
        self.serialize_file = serialize_file
        self.binary_dir = "./binary_logs"
        self.environment = "AWS_FXS"
        self.file_system = "normal" if "FXS" not in self.environment else "luster"
        self.bottleneck_logs = self.check_serialize_file(self.serialize_file)
        self.id_to_attr = {1: 'avg_rtt_value', 2: 'pacing_rate', 3: 'cwnd_rate', 4: 'avg_retransmission_timeout_value', 5: 'byte_ack', 6: 'seg_out', 7: 'retrans', 8: 'mss_value', 9: 'ssthresh_value', 10: 'segs_in', 11: 'avg_send_value', 12: 'unacked_value', 13: 'rcv_space', 14: 'send_buffer_value', 15: 'read_req', 16: 'write_req', 17: 'rkB', 18: 'wkB', 19: 'rrqm', 20: 'wrqm', 21: 'rrqm_perc', 22: 'wrqm_perc', 23: 'r_await', 24: 'w_await', 25: 'aqu_sz', 26: 'rareq_sz', 27: 'wareq_sz', 28: 'svctm', 29: 'util', 30: 'rchar', 31: 'wchar', 32: 'syscr', 33: 'syscw', 34: 'read_bytes_io', 35: 'write_bytes_io', 36: 'cancelled_write_bytes', 37: 'pid', 38: 'ppid', 39: 'pgrp', 40: 'session', 41: 'tty_nr', 42: 'tpgid', 43: 'flags', 44: 'minflt', 45: 'cminflt', 46: 'majflt', 47: 'cmajflt', 48: 'utime', 49: 'stime', 50: 'cutime', 51: 'cstime', 52: 'priority', 53: 'nice', 54: 'num_threads', 55: 'itrealvalue', 56: 'starttime', 57: 'vsize', 58: 'rss', 59: 'rsslim', 60: 'startcode', 61: 'endcode', 62: 'startstack', 63: 'kstkesp', 64: 'kstkeip', 65: 'signal', 66: 'blocked', 67: 'sigignore', 68: 'sigcatch', 69: 'wchan', 70: 'nswap', 71: 'cnswap', 72: 'exit_signal', 73: 'processor', 74: 'rt_priority', 75: 'policy', 76: 'delayacct_blkio_ticks', 77: 'guest_time', 78: 'cguest_time', 79: 'start_data', 80: 'end_data', 81: 'start_brk', 82: 'arg_start', 83: 'arg_end', 84: 'env_start', 85: 'env_end', 86: 'exit_code', 87: 'cpu_usage_percentage', 88: 'mem_usage_percentage', 89: 'tcp_rcv_buffer_min', 90: 'tcp_rcv_buffer_default', 91: 'tcp_rcv_buffer_max', 92: 'tcp_snd_buffer_min', 93: 'tcp_snd_buffer_default', 94: 'tcp_snd_buffer_max', 95: 'req_waittime', 96: 'req_active', 97: 'read_bytes', 98: 'write_bytes', 99: 'ost_setattr', 100: 'ost_read', 101: 'ost_write', 102: 'ost_get_info', 103: 'ost_connect', 104: 'ost_punch', 105: 'ost_statfs', 106: 'ost_sync', 107: 'ost_quotactl', 108: 'ldlm_cancel', 109: 'obd_ping', 110: 'pending_read_pages', 111: 'read_RPCs_in_flight', 112: 'avg_waittime_md1', 113: 'inflight_md1', 114: 'unregistering_md1', 115: 'timeouts_md1', 116: 'req_waittime_md1', 117: 'req_active_md1', 118: 'mds_getattr_md1', 119: 'mds_getattr_lock_md1', 120: 'mds_close_md1', 121: 'mds_readpage_md1', 122: 'mds_connect_md1', 123: 'mds_get_root_md1', 124: 'mds_statfs_md1', 125: 'mds_sync_md1', 126: 'mds_quotactl_md1', 127: 'mds_getxattr_md1', 128: 'mds_hsm_state_set_md1', 129: 'ldlm_cancel_md1', 130: 'obd_ping_md1', 131: 'seq_query_md1', 132: 'fld_query_md1', 133: 'close_md1', 134: 'create_md1', 135: 'enqueue_md1', 136: 'getattr_md1', 137: 'intent_lock_md1', 138: 'link_md1', 139: 'rename_md1', 140: 'setattr_md1', 141: 'fsync_md1', 142: 'read_page_md1', 143: 'unlink_md1', 144: 'setxattr_md1', 145: 'getxattr_md1', 146: 'intent_getattr_async_md1', 147: 'revalidate_lock_md1', 148: 'avg_waittime_md2', 149: 'inflight_md2', 150: 'unregistering_md2', 151: 'timeouts_md2', 152: 'req_waittime_md2', 153: 'req_active_md2', 154: 'mds_getattr_md2', 155: 'mds_close_md2', 156: 'mds_readpage_md2', 157: 'mds_connect_md2', 158: 'mds_statfs_md2', 159: 'mds_sync_md2', 160: 'mds_quotactl_md2', 161: 'mds_getxattr_md2', 162: 'mds_hsm_state_set_md2', 163: 'ldlm_cancel_md2', 164: 'obd_ping_md2', 165: 'seq_query_md2', 166: 'fld_query_md2', 167: 'close_md2', 168: 'create_md2', 169: 'enqueue_md2', 170: 'getattr_md2', 171: 'intent_lock_md2', 172: 'link_md2', 173: 'rename_md2', 174: 'setattr_md2', 175: 'fsync_md2', 176: 'read_page_md2', 177: 'unlink_md2', 178: 'setxattr_md2', 179: 'getxattr_md2', 180: 'intent_getattr_async_md2', 181: 'revalidate_lock_md2', 182: 'label_value'}
        self.datatypes = {  1: 'float', 2: 'string', 3: 'float', 4: 'float', 5: 'float', 6: 'float', 7: 'float', 8: 'float', 9: 'float', 10: 'float', 11: 'string', 12: 'float', 13: 'float', 14: 'float', 15: 'float', 16: 'float', 17: 'float', 18: 'float', 19: 'float', 20: 'float', 21: 'float', 22: 'float', 23: 'float', 24: 'float', 25: 'float', 26: 'float', 27: 'float', 28: 'float', 29: 'float', 30: 'int', 31: 'int', 32: 'int', 33: 'int', 34: 'int', 35: 'int', 36: 'int', 37: 'int', 38: 'int', 39: 'int', 40: 'int', 41: 'int', 42: 'int', 43: 'int', 44: 'int', 45: 'int', 46: 'int', 47: 'int', 48: 'int', 49: 'int', 50: 'int', 51: 'int', 52: 'int', 53: 'int', 54: 'int', 55: 'int', 56: 'int', 57: 'int', 58: 'int', 
                            59: 'intonly', 60: 'intonly', 61: 'intonly', 62: 'intonly', 63: 'int', 64: 'int', 65: 'int', 66: 'int', 67: 'int', 68: 'int', 69: 'int', 70: 'int', 71: 'int', 72: 'int', 73: 'int', 74: 'int', 75: 'int', 76: 'int', 77: 'int', 78: 'int', 79: 'int', 80: 'int', 81: 'int', 82: 'int', 83: 'int', 84: 'int', 85: 'int', 86: 'int', 87: 'float', 88: 'float', 89: 'int', 90: 'int', 91: 'int', 92: 'int', 93: 'int', 94: 'int', 95: 'int', 96: 'int', 97: 'int', 98: 'int', 99: 'int', 100: 'int', 101: 'int', 102: 'int', 103: 'int', 104: 'int', 105: 'int', 106: 'int', 107: 'int', 108: 'int', 109: 'int', 110: 'int', 111: 'int', 112: 'int', 113: 'int', 114: 'int', 115: 'int', 116: 'int', 117: 'int', 118: 'int', 119: 'int', 
                            120: 'int', 121: 'int', 122: 'int', 123: 'int', 124: 'int', 125: 'int', 126: 'int', 127: 'int', 128: 'int', 129: 'int', 130: 'int', 131: 'int', 132: 'int', 133: 'int', 134: 'int', 135: 'int', 136: 'int', 137: 'int', 138: 'int', 139: 'int', 140: 'int', 141: 'int', 142: 'int', 143: 'int', 144: 'int', 145: 'int', 146: 'int', 147: 'int', 148: 'int', 149: 'int', 150: 'int', 151: 'int', 152: 'int', 153: 'int', 154: 'int', 155: 'int', 156: 'int', 157: 'int', 158: 'int', 159: 'int', 160: 'int', 161: 'int', 162: 'int', 163: 'int', 164: 'int', 165: 'int', 166: 'int', 167: 'int', 168: 'int', 169: 'int', 170: 'int', 171: 'int', 172: 'int', 173: 'int', 174: 'int', 175: 'int', 176: 'int', 177: 'int', 178: 'int', 179: 'int', 180: 'int', 181: 'int', 182: 'int'}
        self.filetype = {0: {}, 1: {'read_threads': 1}, 2: {'read_threads': 2}, 3: {'read_threads': 3}, 4: {'read_threads': 4}, 5: {'read_threads': 5}, 6: {'read_threads': 6}, 7: {'read_threads': 7}, 8: {'read_threads': 8}, 9: {'read_threads': 9}, 10: {'read_threads': 10}, 11: {'read_threads': 11}, 12: {'read_threads': 12}, 13: {'read_threads': 13}, 14: {'read_threads': 14}, 15: {'read_threads': 15}, 16: {'read_threads': 16}, 17: {'write_threads': 4}, 18: {'write_threads': 8}, 19: {'write_threads': 12}, 20: {'write_threads': 16}, 21: {'write_threads': 20}, 22: {'write_threads': 24}, 23: {'write_threads': 28}, 24: {'write_threads': 32}, 25: {'write_threads': 36}, 26: {'write_threads': 40}, 27: {'write_threads': 44}, 28: {'write_threads': 48}, 29: {'write_threads': 64}, 30: {'write_threads': 72}, 31: {'write_threads': 96}, 32: {'write_threads': 128}, 33: {'cpu_stress': 2}, 34: {'io_stress': 10}, 35: {'mem_stress': 0.98}, 36: {'link_loss': 0.5}, 37: {'link_loss': 0.1}, 38: {'link_loss': 0.05}, 39: {'link_loss': 1}, 40: {'link_delay': 0.02, "link_delay_distribution": 0}, 41: {'link_delay': 0.03, "link_delay_distribution": 0}, 42: {'link_delay': 0.04, "link_delay_distribution": 0}, 43: {'link_delay': 0.05, "link_delay_distribution": 0}, 44: {'link_duplicate': 0.5}, 45: {'link_duplicate': 0.1}, 46: {'link_duplicate': 0.05}, 47: {'link_duplicate': 1}, 48: {'link_corrupt': 0.5}, 49: {'link_corrupt': 0.1}, 50: {'link_corrupt': 0.05}, 51: {'link_corrupt': 1}, 52: {'link_delay': 1.0, 'link_reorder': 0.5}, 53: {'link_delay': 1.0, 'link_reorder': 0.1}, 54: {'link_delay': 1.0, 'link_reorder': 0.05}, 55: {'link_delay': 1.0, 'link_reorder': 1}}
        self.protobuff_files = {}
        if self.file_system == "normal":
            self.keys = list(range(1, 95)) + [182]
        else:
            self.keys = list(range(1, 15)) + [44, 45, 46, 47, 48, 49, 50, 51, 54, 55, 57, 58, 59, 70, 71, 74, 76, 77, 78] +list(range(87, 183))
        self.network_type = {"DTNS" : 0, "AWS" : 1, "CC" : 2, "AWS_FXS" : 3}
    def read_csv(self, filename):
        data = []
        with open(filename) as csv_file:
            csv_reader = csv.reader(csv_file, delimiter=',')
            count = 0
            for row in csv_reader:
                if count == 0:
                    count += 1
                    continue
                new_row = []
                for i in range(len(row)):
                    type_ = self.datatypes[self.keys[i]]
                    if type_ == "string":
                        new_row.append(self.get_mbps(row[i]))
                    elif type_ == "float":
                        new_row.append(float(row[i]))
                    elif type_ == "intonly":
                        tmp_value = float(row[i])
                        new_row.append(int(tmp_value))
                    else:
                        new_row.append(int(float(row[i])))
                data.append(new_row)
        return data
    def get_mbps(self, thpt):
        thpts = thpt.split("Mb")
        if len(thpts) == 2:
            return float(thpts[0])
        thpts = thpt.split("Kb")
        if len(thpts) == 2:
            return float(thpts[0])/1000.
        thpts = thpt.split("b")
        if len(thpts) == 2:
            return float(thpts[0])/(1000.*1000.)
        thpts = thpt.split("MB")
        if len(thpts) == 2:
            return float(thpts[0])*(1.024*1.024*8)
        thpts = thpt.split("KB")
        if len(thpts) == 2:
            return float(thpts[0])*(1.024*8)/(1000.)
        thpts = thpt.split("B")
        if len(thpts) == 2:
            return float(thpts[0])*(8)/(1000. * 1000.)
        try:
            return float(thpt)
        except:
            return 0.0
    def get_file_id(self, filename):
        number_compile = re.compile('\d+')
        return int(number_compile.findall(filename.split("/")[-1])[0])
    def write_to_proto(self, filename):
        logs = self.read_csv(filename)
        this_file_logs = bottleneck_pb2.BottleneckFile()
        if self.file_system == "normal":
            id_ = self.get_file_id(filename)
            if id_ not in self.protobuff_files:
                self.protobuff_files[id_] = bottleneck_pb2.BottleneckFile()
            this_file_logs = self.protobuff_files[id_]
            attrs = self.filetype[id_]
            for i in attrs:
                this_file_logs.__setattr__(i, attrs[i])
        this_file_logs.__setattr__("network_type", self.network_type[self.environment.upper()])
        this_file_logs.__setattr__("created_time_utc", os.path.getctime(filename))

        length_log = 0 if len(logs) == 0 else len(logs[0])
        for log in logs:
            new_log = bottleneck_pb2.BottleneckLog()
            for i in range(len(self.keys)):
                new_log.__setattr__(self.id_to_attr[self.keys[i]], log[i])
            this_file_logs.rows.append(new_log)
        print("[+] The length of the rows is %d" % len(this_file_logs.rows))
        self.bottleneck_logs.logs.append(this_file_logs)
    
    def write_to_binary(self, serialize_file):
        try:
            file_path = self.binary_dir+"/"+self.environment+"/"+serialize_file
            f = open(file_path, "wb")
            f.write(self.bottleneck_logs.SerializeToString())
            f.close()
            print("Binary file of path: " + file_path + " is created.")
        except IOError:
            print("[+] Error while creating binary file.")

    def add_all_dataset_files(self, folder):
        folder = folder+"/"+self.environment
        files = os.listdir(folder)
        if self.file_system == "normal":
            for filename in files:
                if "dataset" in filename and "csv" in filename:
                    self.write_to_proto(folder+"/"+filename)
        else:
            for filename in files:
                if "csv" in filename and "aws" in filename:
                    self.write_to_proto(folder+"/"+filename)

    def check_serialize_file(self, serialize_file):
        full_path = self.binary_dir + "/" +self.environment+"/" + serialize_file
        bottleneck_logs = bottleneck_pb2.BottleneckFiles()
        if os.path.isfile(full_path):
            try:
                f = open(full_path, "rb")
                bottleneck_logs.ParseFromString(f.read())
                f.close()
            except IOError:
                print(full_path + ": Could not open file.  Creating a new one.")
        return bottleneck_logs

serialize_file = "0"
csv_to_python = CSV_to_Proto(serialize_file)
csv_to_python.add_all_dataset_files("./csv_logs")
csv_to_python.write_to_binary(csv_to_python.serialize_file)