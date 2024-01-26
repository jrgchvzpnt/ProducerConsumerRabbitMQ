import pika
import socket


# Configuración de RabbitMQ
credentials = pika.PlainCredentials(username='guest', password='guest')
parameters = pika.ConnectionParameters(
    host='localhost',
    port=5672,
    virtual_host='/',
    credentials=credentials
)

connection = pika.BlockingConnection(parameters)
channel = connection.channel()

# Nombre de la cola
nombre_de_la_cola = 'InsertarInformacionPCNuevo'

# Declarar la cola si no existe
channel.queue_declare(queue=nombre_de_la_cola)


# Obtener el nombre del equipo
nombre_equipo = socket.gethostname()

# Obtener la dirección IP del equipo
direccion_ip = socket.gethostbyname(socket.gethostname())



# Mensaje que contiene la llamada al procedimiento almacenado de SQL Server
mensaje_sql = f"EXEC InsertarInformacionPC @NombreEquipo='{nombre_equipo}', @DireccionIP='{direccion_ip}'"

# Enviar el mensaje a la cola
channel.basic_publish(exchange='', routing_key=nombre_de_la_cola, body=mensaje_sql)

print(f" [x] Enviado mensaje a la cola: {mensaje_sql}")

# Cerrar la conexión
connection.close()
