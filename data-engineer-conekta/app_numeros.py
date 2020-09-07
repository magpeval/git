
import argparse

#----------------------------------
#Constructor de  argumentos parser 
#-----------------------------------
ap = argparse.ArgumentParser()
ap.add_argument("-a", "--numero", required=True,
   help="first operand")
args = vars(ap.parse_args())



def list_numeros_naturales(numero):
    data = 0
    for i in range(0,101):
        if int(numero) == int(i):
            return int(i)
    return data

numero_extract = 0
numero_extract = list_numeros_naturales(args['numero'])

if numero_extract > 0: 
    print('El número se extrajo correctamente : ' + str(numero_extract))
else:
    data = 0
    print('El número es inválido, no se encuentra en el rango [1:100] : ' + str(numero_extract))