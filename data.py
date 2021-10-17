from dataclasses import dataclass
@dataclass
class Data:
    """Important data"""
    ID: int
    title: str
    subtitle: str
    autor: str
    abstract: str

    def __init__(self, ID: int, title: str, subtitle: str, autor: str, abstract: str):
        self.ID = ID
        self.title = title # in grams, for calculating shipping
        self.subtitle = subtitle
        self.autor = autor
        self.abstract = abstract

    def __str__(self):
        return 'ID: ' + str(self.ID) + '\n' + 'Autor: ' + self.autor + '\n' + 'Title: ' + self.title + '\n' + 'Subtitle: ' + self.subtitle + '\n' + 'Abstrakt:' + self.abstract

    @property
    def merge(self):
        return ' '.join([self.title, self.subtitle, self.abstract])