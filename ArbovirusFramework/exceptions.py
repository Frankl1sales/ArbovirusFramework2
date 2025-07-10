class ArbovirusFrameworkError(Exception):
    """Exceção base para o ArbovirusFramework."""
    pass

class FileNotFoundError(ArbovirusFrameworkError):
    """Levantada quando um arquivo especificado não é encontrado."""
    pass

class InvalidFileFormatError(ArbovirusFrameworkError):
    """Levantada quando um arquivo está em um formato inválido (não CSV)."""
    pass

class InvalidCSVError(ArbovirusFrameworkError):
    """Levantada quando um arquivo CSV está malformado ou ilegível."""
    pass

class ColumnNotFoundError(ArbovirusFrameworkError):
    """Levantada quando uma ou mais colunas especificadas não existem no DataFrame."""
    pass

class InvalidTransformationError(ArbovirusFrameworkError):
    """Levantada quando uma transformação não pode ser aplicada."""
    pass