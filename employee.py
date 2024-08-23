import json
import math

class Employee:

    def __init__(self, dpt: str = '', div: str = '', pos: str = '', hr_date: str = '', salary: float = 0):
        self.dpt = dpt
        self.div = div
        self.pos = pos
        self.hr_date = hr_date
        self.salary = math.floor(salary)

    @staticmethod
    def from_csv_line(line):
        return Employee(line[0], line[1], line[3], line[5], float(line[7]))

    def to_json(self):
        return json.dumps(self.__dict__)
