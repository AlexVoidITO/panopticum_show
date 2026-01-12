from typing import List, Dict


data = [{1: [230.0, 84.49, 19002.0, 0.0]}, {2: [228.732, 7.15, 1635.0, 0.015]}, {3: [227.572, 6.15, 1635.0, 0.015]}, {4: [226.504, 3.65, 827.0, 0.015]}, {5: [225.491, 4.18, 941.0, 0.015]}, {6: [224.54, 4.78, 1073.0, 0.015]}, {7: [223.661, 3.99, 893.0, 0.015]}, {8: [222.842, 2.93, 653.0, 0.015]}, {9: [222.067, 4.72, 1049.0, 0.015]}, {10: [221.362, 4.61, 1021.0, 0.015]}, {11: [220.727, 5.13, 1133.0, 0.015]}, {12: [220.169, 4.89, 1077.0, 0.015]}, {13: [219.684, 4.22, 928.0, 0.015]}, {14: [219.263, 4.77, 1045.0, 0.015]}, {15: [218.913, 3.84, 840.0, 0.015]}, {16: [218.621, 4.55, 995.0, 0.015]}, {17: [218.397, 4.85, 1060.0, 0.015]}, {18: [218.246, 3.26, 710.0, 0.015]}, {19: [218.144, 6.82, 1487.0, 0.015]}]  


def sum_by_index(data: List[Dict[int, List[float]]], index: int) -> float:
    return sum(list(d.values())[0][index] for d in data)

def analyze_points(points: List[Dict[int, List[float]]]) -> Dict[str, float | int]:
    deltas = []

    for i in range(1, len(points) - 1):  # не берём последний дом — у него нет "следующего"
        current = points[i]
        next_home = points[i + 1]

        home_id, current_data = list(current.items())[0]
        next_data = list(next_home.values())[0]

        current_volts = current_data[0]
        current_resistance = current_data[3]

        next_volts = next_data[0]

        # Разница вольт между текущим и следующим домом
        volt_diff = (current_volts - next_volts) ** 2
        volt_power = volt_diff / current_resistance if current_resistance != 0 else 0

        # Берём только дома после следующего
        future_points = points[i + 2:]
        future_ampers_sum = sum_by_index(future_points, index=1)

        if future_ampers_sum == 0 or current_resistance == 0:
            delta = 0
        else:
            amper_power = (future_ampers_sum ** 2) * current_resistance
            delta = volt_power / amper_power

        deltas.append({"delta": delta, "home_id": home_id})

    return max(deltas, key=lambda x: x["delta"])




delt = analyze_points(data)
print(delt)        