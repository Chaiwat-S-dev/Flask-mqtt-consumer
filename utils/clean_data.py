import re

def has_required_fields(message):
    keys_list = ['humidity', 'temperature', 'water_leak', 'adc1', 'adc2', 'chn25', 'chn26', 'battery', 'temp']

    if message.get('object', False):
        for key in message['object'].keys():
            if key in keys_list:
                return True
    return False

def make_influx_data(data):
    """
    raw_data = {
        "measurement": data['applicationName'],
        "tags": {
            "deviceName": data['deviceName'],
            "mac_address": data['devEUI'],
            "devEUI": data['devEUI'],
            "applicationName": data['applicationName'],
            "applicationID": data['applicationID']
        },
        "time": datetime.datetime.now(),
        "fields": {
            "humidity": data['object']['humidity'],
            "temperature": data['object']['temperature'],
            "water_leak": data['object']['water_leak'],
        }
    }
    """

    fields = {}
    json_body = []
    raw_data = {
        "measurement": "chirpstack",
    }

    for key, value in data['object'].items():
        raw_data.update({
            "tags": {
                "deviceName": data['deviceName'],
                "mac_address": data['devEUI'],
                "devEUI": data['devEUI'],
                "applicationName": data['applicationName'],
                "applicationID": data['applicationID']
            }
        })
        if key == 'water_leak':
            fields[key] = 1 if value == 'leak' else 0
        elif key in ['din1', 'dout1']:
            fields[key] = 0 if value == 'off' else 1
        elif key in ['adc1', 'adc2']:
            raw_data['tags'].update({"adc": key})
            
            adc_fields = {}
            for adc_field, adc_value in value.items():
                adc_fields[adc_field] = float(adc_value)
            
            json_body.append({
                **raw_data,
                "fields": adc_fields
            })
        elif key in ['chn25', 'chn26']:
            fields.update({key: value})
        else:
            try:
                if isinstance(value, (int, float)):
                    fields.update({key: float(value)})
                elif isinstance(value, str) and value.replace('.', '', 1).isdigit():
                    fields.update({key: float(value)})
                else:
                    print(f"ValueErrorType: key = '{key}' value = '{value}', value_type: '{type(value)}'")
                    continue
                
            except ValueError:
                print(f"KeyError: key = '{key}' value = '{value}', value_type: '{type(value)}'")

    if fields:
        json_body.append({
            **raw_data,
            "fields": fields
        })
    
    return json_body

def imonit_clean_data(message):
    fields, raw_data = {}, {}
    if (plot_lables := message.get('plotLabels', None)) and (plot_values := message.get('plotValues', None)):           
        raw_fields = [ change_label_to_variable(i) for i in plot_lables.split('|') ]
        raw_values = [float(i) if i else 0 for i in plot_values.split('|')]
        data = dict(zip(raw_fields, raw_values))
        fields.update(data)

    # update battery level
    if battery_level := message.get('batteryLevel'):
        fields.update({ "battery_level": float(battery_level) })

    if fields:
        raw_data = {
            "measurement": "imonit",
            "tags": {
                "mac_address": message.get('sensorID'),
                "gateway_id": message.get("gateway_id"),
                "network_id": message.get("network_id"),
            },
            "fields": fields
        }

    return raw_data

def temphumid_make_data(data):
    join_int_with_float = lambda a, b : float(str(a) + "." + str(b))
        
    payload_mac_address = data.get('mac', "")
    json_data = {
        "measurement": "temphumid",
        "timestamp": data.get('timestamp', ""),
        "tags": {
            "mac_address": payload_mac_address,
            "gateway": data.get('gateway', "")
        }
    }
    
    if raw_code := data.get('rawData'):

        range_step_offset = [raw_code[i:i+2] for i in range(0, len(raw_code), 2)]
        frame_version = range_step_offset[8]

        # frame_version => 00 is Static Frames => Battery Level
        if frame_version == "00":
            battery_level = int(range_step_offset[15], base=16)

            json_data.update({
                "fields": {
                    "battery_level": battery_level
                }
            })

        # frame_version => 05 is Temperature and Humidity Frames
        elif frame_version == "05":
            # device_name = "".join(range_step_offset[16:24])

            temperature_int = int(range_step_offset[12], base=16)
            temperature_point = int(range_step_offset[13], base=16)
            temperature = join_int_with_float(temperature_int, temperature_point)

            humidity_int = int(range_step_offset[14], base=16)
            humidity_point = int(range_step_offset[15], base=16)
            humidity = join_int_with_float(humidity_int, humidity_point)

            json_data.update({
                "fields": {
                    "temperature": temperature,
                    "humidity": humidity
                }
            })
    
    return json_data if json_data.get('fields') else {}

def change_label_to_variable(label):
  """Changes a label to a variable name in PEP8 format.

  Args:
    label (str): The label to be changed.

  Returns:
    str: The variable name in PEP8 format.
  """

  label = label.lower()
  label = re.sub("-", "_", label)
  label = re.sub(" ", "_", label)
  return label

def advantech_make_data(gateway: str, mac_address: str, fields_object: dict):
    return [{
        "measurement": "advantech",
        "tags": {
            "mac_address": mac_address,
            "gateway": gateway,
        },
        "fields": fields_object
    }]