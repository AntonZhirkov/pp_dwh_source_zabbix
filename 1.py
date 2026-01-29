class WeatherStation:
    _meteo_attr = {
        "temperature": 0, 
        "humidity": 0, 
        "pressure": 0
    }
    
    def __init__(self):
        self.__dict__ = WeatherStation._meteo_attr
    
    def update_data(self, **kwargs):
        self.__dict__.update(**kwargs)
        
    def get_current_data(self):
        return tuple(self.__dict__.values())
    
a = WeatherStation()
print(a.get_current_data())