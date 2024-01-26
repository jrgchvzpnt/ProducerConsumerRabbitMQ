import pika
import pyodbc

# Definir funciones

def ejecutar_procedimiento_almacenado(mensaje):
    try:
        with sql_server_conn.cursor() as cursor:
            cursor.execute(mensaje)
        sql_server_conn.commit()
        print("Procedimiento almacenado ejecutado con éxito.")
    except Exception as e:
        print(f"Error al ejecutar el procedimiento almacenado: {str(e)}")

def callback(ch, method, properties, body):
    # Decodificar el mensaje y ejecutar el procedimiento almacenado
    mensaje = body.decode('utf-8')
    try:
        ejecutar_procedimiento_almacenado(mensaje)
        print(mensaje)
    except Exception as e:
        print(f"Error durante el procesamiento del mensaje: {str(e)}")

# Configuración de RabbitMQ
rabbitmq_credentials = pika.PlainCredentials(username='guest', password='guest')
rabbitmq_parameters = pika.ConnectionParameters(
    host='localhost',
    port=5672,
    virtual_host='/',
    credentials=rabbitmq_credentials
)
rabbitmq_connection = pika.BlockingConnection(rabbitmq_parameters)
rabbitmq_channel = rabbitmq_connection.channel()
rabbitmq_channel.queue_declare(queue='InsertarInformacionPCNuevo')

# Configuración de SQL Server
sql_server_conn_str = 'DRIVER={SQL Server};SERVER=10.0.1.219;DATABASE=SisPrevencion;Integrated Security=True;'
sql_server_conn = pyodbc.connect(sql_server_conn_str)

# Función de callback para procesar mensajes

# Configurar el consumidor para escuchar la cola y llamar al callback
rabbitmq_channel.basic_consume(queue='InsertarInformacionPCNuevo', on_message_callback=callback, auto_ack=True)

# Iniciar la escucha de mensajes
print('Esperando mensajes. Para salir presiona CTRL+C')
try:
    rabbitmq_channel.start_consuming()
except KeyboardInterrupt:
    print('Deteniendo el consumidor mediante interrupción de teclado.')
finally:
    rabbitmq_channel.close()
    rabbitmq_connection.close()
    sql_server_conn.close()
