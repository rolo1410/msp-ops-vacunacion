def validar_cedula_ecuatoriana(cedula: str) -> bool:
    """
    Valida el formato y el dígito verificador de una cédula de identidad ecuatoriana.
    Utiliza el algoritmo "Módulo 10".
    
    :param cedula: La cadena de 10 dígitos de la cédula.
    :return: True si la cédula es válida, False en caso contrario.
    """
    
    # 1. Validar longitud y que sean dígitos
    if not cedula.isdigit() or len(cedula) != 10:
        return False

    # Convertir a lista de enteros para facilitar las operaciones
    digitos = [int(d) for d in cedula]
    
    # Extraer el último dígito (dígito verificador)
    ultimo_digito = digitos[9]
    
    # Extraer los primeros 9 dígitos para el cálculo
    coeficientes = [2, 1, 2, 1, 2, 1, 2, 1, 2] # Coeficientes a usar
    suma = 0
    
    # 2. Validar código de provincia (primeros dos dígitos)
    codigo_provincia = digitos[0] * 10 + digitos[1]
    
    # Se considera válido hasta el 24 (provincias) o el 30 (ecuatorianos en el exterior)
    if not (1 <= codigo_provincia <= 24 or codigo_provincia == 30):
         return False

    # 3. Validar el tercer dígito (tipo de persona: < 6 para personas naturales)
    # Excluye 6 (públicas) y 9 (privadas) que corresponden a RUC.
    if digitos[2] >= 6 and codigo_provincia != 30: # El 30 no tiene esta restricción de forma obligatoria
        return False

    # 4. Aplicar Módulo 10 a los primeros 9 dígitos
    for i in range(9):
        # Multiplicar el dígito por su coeficiente
        valor = digitos[i] * coeficientes[i]
        
        # Si el resultado es mayor a 9, se le resta 9
        if valor >= 10:
            valor -= 9
        
        # Acumular la suma
        suma += valor

    # 5. Calcular el dígito verificador esperado
    # Obtener el residuo (módulo 10) de la suma
    residuo = suma % 10
    
    # La decena inmediata superior (o múltiplo de 10) menos la suma
    # Equivale a 10 - residuo (si residuo > 0)
    if residuo == 0:
        # Si el residuo es 0, el dígito verificador es 0
        digito_esperado = 0
    else:
        # 10 - residuo
        digito_esperado = 10 - residuo
        
    # 6. Comparar el dígito verificador calculado con el dígito real
    return digito_esperado == ultimo_digito
