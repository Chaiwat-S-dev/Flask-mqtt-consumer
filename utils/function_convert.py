import struct
import math

def int_of_hex_to_float(point_hex: int, int_hex: int):
    if point_hex >= 65536 or int_hex >= 65536:
        return 0.00

    hex_str = "{:04x}{:04x}".format(int_hex, point_hex)
    float_num = struct.unpack('!f', bytes.fromhex(hex_str))[0]
    result = round(float_num,2) if not math.isnan(float_num) else 0.00
    return result


def int_of_hex_to_int(register1: int, register2 :int):
    if register1 >= 65536 or register2 >= 65536:
        return 0
    
    hex_str = "0x{:04x}{:04x}".format(register2, register1)
    return int(hex_str, 16)


def flow_rate_with_decimal(register1: int, register2: int, register3: int, register4: int):
    int_num = int_of_hex_to_int(register1, register2)
    float_num = int_of_hex_to_float(register3, register4)
    total_num = int_num + float_num
    if total_num >= 4000000000:
        total_num = total_num - 4294967295
    return round(total_num, 2)


def scale_number(actaul_number, in_min, in_max, out_min, out_max):
    result = (actaul_number - in_min) * (out_max - out_min) / (in_max - in_min) + out_min
    return round(result, 2)
