class Point:
    points = []

    def __init__(self, x, y):
        self.x = x
        self.y = y
        Point.points.append(self)

    def get_distance_to_origin(self):
        return (self.x ** 2 + self.y ** 2) ** 0.5


    def get_distance(self, another_point):
        if not isinstance(another_point, Point):
            print('Передана не точка')
            return None
        return ((self.x - another_point.x) ** 2
                + (self.y - another_point.y) ** 2) ** 0.5
    
    def get_point_with_max_distance(self):
        max_point = max(self.points, key=lambda point: (point.get_distance_to_origin(), point.y))
        max_point.display()

    def display(self):
        print(f"Point({self.x}, {self.y})")
        
p1 = Point(2, 4)
p2 = Point(6, 7)
p3 = Point(10, 5)
p1.get_point_with_max_distance()
