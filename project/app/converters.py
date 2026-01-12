def normalize_dict_to_list(data:dict) -> list:
    data_values = []
    for point, nested_dict in data.items():
        values = [point]
        for value in nested_dict.values():
            values.append(value)
        data_values.append(values)
    return data_values

def extract_home_data(points: list) -> list:
    print(points)
    data_points = []
    for point in points:
        home_id = point["home_id"]  
        volts = point["volts"]
        ampers = point["ampers"] 
        power = point["power"]
        resistance = point["resistance"] # участком сопротивления является провод, находящийся по схеме за домом
        home_num = point["home_num"]
        data_points.append({home_id: [volts, ampers, power, resistance,home_num]})
    return data_points
