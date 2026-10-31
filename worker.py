#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
worker.py - Nodo de Trabajo (Worker / Agente).

Taller de Sockets - Sistemas Distribuidos (UPS).

Responsabilidades del Worker:
  1. Latidos UDP   : envia un paquete PING cada 2 s al Master con su estado.
  2. Sesion TCP    : mantiene una conexion persistente con el Master; recibe
                     comandos y responde con informes estructurados.
  3. Tareas        : ejecuta las instrucciones (EXEC_TASK) en segundo plano,
                     sin congelar la conexion TCP.
  4. Rafaga        : ante BROADCAST_STRESS lanza una simulacion de carga
                     concurrente contra un objetivo LOCAL y reporta metricas.

Seguridad (Actividad 3):
  La rafaga solo se ejecuta contra objetivos de loopback (127.0.0.0/8) o de red
  privada (RFC 1918). Si el objetivo es una direccion publica, el Worker la
  rechaza. Esto coincide con el entregable, que pide probar en "maquina local o
  red virtual", y evita convertir la practica en una herramienta de abuso.

Uso:
  python3 worker.py --id worker-1 --master 127.0.0.1
  python3 worker.py --id worker-2 --master 192.168.1.50 --tcp-port 6000 --udp-port 6001
"""

import argparse
import ipaddress
import random
import socket
import threading
import time

# ---------------------------------------------------------------------------
# Configuracion por defecto
# ---------------------------------------------------------------------------
DEFAULT_MASTER = "127.0.0.1"
DEFAULT_TCP_PORT = 6000
DEFAULT_UDP_PORT = 6001

HEARTBEAT_INTERVAL = 2.0   # segundos entre latidos (lo pide la Actividad 1)
DELIMITER = "\n"           # delimitador de mensajes en el canal TCP

# Limites de seguridad para la rafaga de carga simulada.
MAX_HILOS_RAFAGA = 20      # hilos concurrentes maximos por Worker
MAX_DURACION = 30          # segundos maximos de rafaga, aunque pidan mas
PAUSA_ENTRE_PETICIONES = 0.01  # pequena pausa: es una simulacion, no un flood

shutdown_event = threading.Event()


# ===========================================================================
# ACTIVIDAD 1: envio de latidos (UDP)
# ===========================================================================
def leer_cpu_simulada():
    """
    Devuelve un porcentaje de uso de CPU.

    Si 'psutil' esta instalado usamos la lectura real; si no, simulamos un
    valor para no depender de librerias externas (el taller no las exige).
    """
    try:
        import psutil  # dependencia opcional
        return f"{psutil.cpu_percent(interval=None):.0f}%"
    except Exception:
        return f"{random.randint(5, 95)}%"


def enviar_latidos(node_id, master_host, udp_port):
    """
    Envia un latido periodico al Master:
        PING:<node_id>|<cpu_usage>|<status>
    y espera (sin bloquear mas de 1 s) el ACK de confirmacion.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.settimeout(1.0)
    print(f"[UDP] Enviando latidos a {master_host}:{udp_port} cada "
          f"{HEARTBEAT_INTERVAL}s")

    while not shutdown_event.is_set():
        cpu = leer_cpu_simulada()
        trama = f"PING:{node_id}|{cpu}|ALIVE"
        try:
            sock.sendto(trama.encode("utf-8"), (master_host, udp_port))
            # Intentamos leer el ACK (no es obligatorio para seguir vivos).
            try:
                data, _ = sock.recvfrom(1024)
                # Descomenta para depurar el ACK recibido:
                # print(f"[UDP] {data.decode(errors='ignore')}")
            except socket.timeout:
                pass
        except OSError as e:
            print(f"[UDP] Error enviando latido: {e}")
        # Esperamos el intervalo, pero reaccionando rapido al apagado.
        shutdown_event.wait(HEARTBEAT_INTERVAL)

    sock.close()
    print("[UDP] Latidos detenidos")


# ===========================================================================
# ACTIVIDAD 2: tareas bajo demanda (EXEC_TASK)
# ===========================================================================
def ejecutar_tarea(tarea):
    """
    Simula la ejecucion de una tarea solicitada por el Master y devuelve un
    resultado en texto. Cada tarea corre en su propio hilo (ver abajo), por lo
    que una tarea lenta NO congela la conexion TCP del Worker.
    """
    if tarea == "scan_ports":
        # Simulamos un escaneo de puertos locales de uso comun.
        puertos = [22, 80, 443, 3306, 5432, 6000]
        abiertos = []
        for p in puertos:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(0.2)
            try:
                if s.connect_ex(("127.0.0.1", p)) == 0:
                    abiertos.append(p)
            except OSError:
                pass
            finally:
                s.close()
        return f"puertos_abiertos={abiertos}"

    elif tarea == "fetch_logs":
        # Simulamos la recoleccion de un resumen de logs.
        time.sleep(1.0)  # trabajo "pesado" simulado
        return "logs=OK lineas=1532 errores=3"

    else:
        # Tarea generica desconocida: devolvemos un eco.
        time.sleep(0.5)
        return f"tarea_desconocida='{tarea}' ejecutada"


def lanzar_tarea_en_segundo_plano(conn, node_id, tarea):
    """
    Ejecuta la tarea en un hilo aparte y, al terminar, envia el informe:
        REPORT:<node_id>|<tarea>|<resultado>
    """
    def trabajo():
        print(f"[TAREA] Ejecutando '{tarea}'...")
        resultado = ejecutar_tarea(tarea)
        informe = f"REPORT:{node_id}|{tarea}|{resultado}{DELIMITER}"
        try:
            conn.sendall(informe.encode("utf-8"))
            print(f"[TAREA] Informe enviado: {resultado}")
        except OSError as e:
            print(f"[TAREA] No se pudo enviar el informe: {e}")

    threading.Thread(target=trabajo, daemon=True).start()


# ===========================================================================
# ACTIVIDAD 3: rafaga sincronizada (simulacion de carga)
# ===========================================================================
def objetivo_es_seguro(host):
    """
    Valida que el objetivo de la rafaga sea una direccion local o privada.
    Devuelve True solo para loopback o rangos RFC 1918 (red del laboratorio).
    """
    try:
        ip = ipaddress.ip_address(socket.gethostbyname(host))
    except (ValueError, OSError):
        return False
    return ip.is_loopback or ip.is_private


def rafaga_de_carga(conn, node_id, host, port, duracion):
    """
    Genera carga concurrente controlada contra <host>:<port> durante
    <duracion> segundos y mide latencia y numero de peticiones.

    Al terminar envia:
        STRESS_REPORT:<node_id>|requests=<n>|avg_ms=<x>|errors=<e>
    """
    # ---- Validaciones de seguridad y limites ----
    if not objetivo_es_seguro(host):
        aviso = (f"STRESS_REPORT:{node_id}|RECHAZADO|motivo="
                 f"objetivo_no_local_{host}{DELIMITER}")
        try:
            conn.sendall(aviso.encode("utf-8"))
        except OSError:
            pass
        print(f"[RAFAGA] Objetivo {host} rechazado (no es local/privado)")
        return

    duracion = min(max(1, duracion), MAX_DURACION)  # acotamos la duracion

    # Contadores compartidos entre los hilos de la rafaga.
    estado = {"requests": 0, "errors": 0, "latencias": []}
    estado_lock = threading.Lock()
    fin = time.time() + duracion

    def trabajador_carga():
        """Cada hilo abre conexiones repetidas y mide la latencia."""
        while time.time() < fin and not shutdown_event.is_set():
            inicio = time.time()
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(1.0)
            try:
                s.connect((host, port))
                s.sendall(b"LOAD")         # payload minimo de prueba
                s.recv(64)                  # esperamos la respuesta del objetivo
                ms = (time.time() - inicio) * 1000.0
                with estado_lock:
                    estado["requests"] += 1
                    estado["latencias"].append(ms)
            except OSError:
                with estado_lock:
                    estado["errors"] += 1
            finally:
                s.close()
            time.sleep(PAUSA_ENTRE_PETICIONES)  # simulacion, no saturacion

    print(f"[RAFAGA] Iniciando carga a {host}:{port} por {duracion}s "
          f"con {MAX_HILOS_RAFAGA} hilos")
    hilos = [threading.Thread(target=trabajador_carga, daemon=True)
             for _ in range(MAX_HILOS_RAFAGA)]
    for h in hilos:
        h.start()
    for h in hilos:
        h.join()

    # Calculo de metricas finales.
    with estado_lock:
        total = estado["requests"]
        errores = estado["errors"]
        prom = (sum(estado["latencias"]) / len(estado["latencias"])
                if estado["latencias"] else 0.0)

    informe = (f"STRESS_REPORT:{node_id}|requests={total}|"
               f"avg_ms={prom:.2f}|errors={errores}{DELIMITER}")
    try:
        conn.sendall(informe.encode("utf-8"))
    except OSError:
        pass
    print(f"[RAFAGA] Terminada: {total} peticiones, "
          f"{prom:.2f} ms promedio, {errores} errores")


# ===========================================================================
# Sesion TCP persistente con el Master
# ===========================================================================
def leer_lineas(conn):
    """Lee del socket y entrega mensajes completos delimitados por '\\n'."""
    buffer = ""
    while not shutdown_event.is_set():
        try:
            datos = conn.recv(4096)
        except OSError:
            break
        if not datos:
            break
        buffer += datos.decode("utf-8", errors="ignore")
        while DELIMITER in buffer:
            linea, buffer = buffer.split(DELIMITER, 1)
            if linea:
                yield linea


def sesion_tcp(node_id, master_host, tcp_port):
    """
    Mantiene la sesion TCP con el Master. Si la conexion se cae, reintenta
    conectarse (tolerancia a fallos basica). Procesa los comandos entrantes.
    """
    while not shutdown_event.is_set():
        conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            conn.connect((master_host, tcp_port))
        except OSError as e:
            print(f"[TCP] No se pudo conectar al Master ({e}); "
                  f"reintento en 3 s")
            conn.close()
            shutdown_event.wait(3.0)
            continue

        # Registro inicial: avisamos al Master quienes somos.
        try:
            conn.sendall(f"REGISTER:{node_id}{DELIMITER}".encode("utf-8"))
            print(f"[TCP] Sesion establecida con el Master "
                  f"{master_host}:{tcp_port}")
        except OSError:
            conn.close()
            continue

        # Bucle de recepcion de comandos.
        try:
            for mensaje in leer_lineas(conn):
                if mensaje.startswith("EXEC_TASK:"):
                    tarea = mensaje[len("EXEC_TASK:"):].strip()
                    # Se ejecuta en segundo plano: no bloquea este bucle.
                    lanzar_tarea_en_segundo_plano(conn, node_id, tarea)

                elif mensaje.startswith("BROADCAST_STRESS:"):
                    # BROADCAST_STRESS:<host>:<port>:<duracion>
                    cuerpo = mensaje[len("BROADCAST_STRESS:"):]
                    try:
                        host, port, dur = cuerpo.split(":")
                        port, dur = int(port), int(dur)
                    except ValueError:
                        print(f"[TCP] Orden de rafaga malformada: {cuerpo}")
                        continue
                    # La rafaga corre en su propio hilo para no congelar la
                    # recepcion de otros comandos.
                    threading.Thread(
                        target=rafaga_de_carga,
                        args=(conn, node_id, host, port, dur),
                        daemon=True).start()

                else:
                    print(f"[TCP] Comando no reconocido: {mensaje}")
        except Exception as e:
            print(f"[TCP] Error en la sesion: {e}")
        finally:
            conn.close()

        if not shutdown_event.is_set():
            print("[TCP] Conexion perdida; reintentando en 3 s")
            shutdown_event.wait(3.0)


# ===========================================================================
# Arranque
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="Worker - Taller de Sockets")
    parser.add_argument("--id", required=True, help="Identificador del nodo")
    parser.add_argument("--master", default=DEFAULT_MASTER,
                        help="Host/IP del Master")
    parser.add_argument("--tcp-port", type=int, default=DEFAULT_TCP_PORT)
    parser.add_argument("--udp-port", type=int, default=DEFAULT_UDP_PORT)
    args = parser.parse_args()

    print(f"=== WORKER '{args.id}' ===")

    hilos = [
        threading.Thread(target=enviar_latidos,
                         args=(args.id, args.master, args.udp_port),
                         daemon=True),
        threading.Thread(target=sesion_tcp,
                         args=(args.id, args.master, args.tcp_port),
                         daemon=True),
    ]
    for h in hilos:
        h.start()

    # Mantenemos vivo el hilo principal hasta Ctrl+C.
    try:
        while not shutdown_event.is_set():
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[WORKER] Apagando...")
        shutdown_event.set()
        time.sleep(1.0)
        print("[WORKER] Recursos liberados.")


if __name__ == "__main__":
    main()
