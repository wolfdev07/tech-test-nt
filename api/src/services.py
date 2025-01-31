class NumberSet:
    def __init__(self):
        self.original_numbers = set(range(1, 101))
        self.numbers = set(self.original_numbers)

    def extract(self, n: int):
        if n not in self.numbers:
            raise ValueError("Número inválido o ya eliminado")
        self.numbers.remove(n)
    
    def find_missing(self) -> list:
        removed = list(self.original_numbers - self.numbers)
        if len(removed) < 1:
            raise ValueError("No hay números faltantes")
        return removed