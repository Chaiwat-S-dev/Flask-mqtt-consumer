from utils.function_convert import int_of_hex_to_float, int_of_hex_to_int, flow_rate_with_decimal, scale_number


def get_topic_listening_list(data: dict) -> list:
    topic_listening_list = []
    for root_topic, gateway_list in data.items():
        for gateway_object in gateway_list:
            gateway = gateway_object.get('gateway', None)
            for device_object in gateway_object.get('device_list', []):
                mac_address = device_object.get('mac_address', None)
                if gateway and mac_address:
                    device_topic = f"{root_topic}/{gateway}/{mac_address}/data"
                    topic_listening_list.append(device_topic)

    return topic_listening_list

def get_handle_device_params(data: dict) -> dict:
    device_params_object = {}
    for root_topic, gateway_list in data.items():
        for gateway_object in gateway_list:
            gateway = gateway_object.get('gateway', None)
            for device_object in gateway_object.get('device_list', []):
                mac_address = device_object.get('mac_address', None)
                if gateway and mac_address:
                    device_topic = f"{root_topic}/{gateway}/{mac_address}/data"
                    device_params = device_object.get('parameter_list', [])
                    device_params_object[device_topic] = device_params
    return device_params_object


def make_arguments(argument_list: list, messages: dict):
    clean_argument_list = []
    for arg in argument_list:
        if isinstance(arg, str):
            clean_arg = messages.get(arg)
        else:
            clean_arg = arg
        clean_argument_list.append(clean_arg)

    return clean_argument_list


def handle_callback(func_opperate: str) -> callable:
    match func_opperate:
        case "int_of_hex_to_float":
            func_calback = int_of_hex_to_float
        case "int_of_hex_to_int":
            func_calback = int_of_hex_to_int
        case "flow_rate_with_decimal":
            func_calback = flow_rate_with_decimal
        case "scale_number":
            func_calback = scale_number
        case _:
            func_calback = None
    return func_calback


def handle_advantech(topic: str, message: dict, advantech_device_cofig: dict) -> dict:
    data = {}
    get_device_params = get_handle_device_params(advantech_device_cofig)
    if topic in get_device_params:
        get_params_list = get_device_params[topic]
        for param_object in get_params_list:
            if all(
                    (arg in message.keys() or isinstance(arg, (int, float))) for arg in param_object.get('arguments', [])
                ):
                parameter = param_object.get('parameter', None)
                func = param_object.get('function', None)
                func_calback = handle_callback(func)
                arguments = param_object.get('arguments', [])
                clean_arguments = make_arguments(arguments, message)

                if parameter and func_calback:
                    sensor_value = func_calback(*clean_arguments)

                    if equation := param_object.get('equation', None):
                        sensor_value = eval(equation, {"value": sensor_value})

                    data[parameter] = sensor_value

    return data