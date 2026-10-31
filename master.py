#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
master.py - Nodo Central (Master) de Comando y Control.

Taller de Sockets - Sistemas Distribuidos (UPS).

Responsabilidades del Master:
  1. Servidor UDP  : recibe los latidos (heartbeats) de los Workers, mantiene
                     una tabla global de nodos y detecta automaticamente los
                     nodos caidos (sin latidos por mas de 6 s).
  2. Servidor TCP  : multihilo (un hilo por Worker). Mantiene una sesion
                     persistente con cada Worker y permite enviarle comandos
                     en tiempo real, recibiendo sus informes de vuelta.
  3. CLI de control: interfaz de linea de comandos para que el operador liste
                     nodos, despache tareas y lance la rafaga sincronizada.

Nota sobre la Actividad 3 (rafaga de estres):
  Esta es una SIMULACION DE CARGA CONTROLADA contra un servicio propio en la
  maquina local o la red virtual del laboratorio. Por seguridad, el Worker solo
  acepta objetivos en direcciones de loopback (127.0.0.0/8) o de red privada
  (RFC 1918), que es exactamente el escenario que pide el entregable
  ("maquina local o red virtual"). El Master incluye un comando `target` que
  levanta un servidor de prueba local para usar como objetivo simulado.

Uso:
  python3 master.py                 # usa host/puertos por defecto
  python3 master.py --host 0.0.0.0 --tcp-port 6000 --udp-port 6001
"""

import argparse
import socket
import threading
import time

# ---------------------------------------------------------------------------
# Configuracion por defecto
# ---------------------------------------------------------------------------
DEFAULT_HOST = "0.0.0.0"     # escuchar en todas las interfaces locales
DEFAULT_TCP_PORT = 6000      # puerto del canal de comando y control (TCP)
DEFAULT_UDP_PORT = 6001      # puerto del canal de latidos (UDP)

HEARTBEAT_TIMEOUT = 6.0      # segundos sin latido -> nodo "Caido"
MONITOR_INTERVAL = 1.0       # cada cuanto el monitor revisa la tabla de nodos
DELIMITER = "\n"             # delimitador de mensajes en el canal TCP

# ---------------------------------------------------------------------------
# Estado global compartido entre hilos
# ---------------------------------------------------------------------------
# Tabla de nodos conocidos por UDP:
#   node_id -> {"cpu": str, "status": str, "last_seen": float,
#               "alive": bool, "udp_addr": (ip, port)}
nodes = {}
nodes_lock = threading.Lock()  # protege el acceso concurrente a 'nodes'

# Clientes TCP conectados (sesiones persistentes):
#   node_id -> {"conn": socket, "addr": (ip, port)}
tcp_clients = {}
tcp_lock = threading.Lock()    # protege el acceso concurrente a 'tcp_clients'

# Evento para apagar ordenadamente todos los hilos.
shutdown_event = threading.Event()


# ===========================================================================
# ACTIVIDAD 1: Canal de latidos y registro (UDP)
# ===========================================================================
def udp_server(host, udp_port):
    """
    Servidor UDP que recibe los latidos de los Workers.

    Trama esperada:  PING:<node_id>|<cpu_usage>|<status>
    Respuesta:       ACK:<node_id>
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # Reusar el puerto evita el error "Address already in use" al reiniciar.
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, udp_port))
    sock.settimeout(1.0)  # timeout para poder revisar shutdown_event
    print(f"[UDP] Canal de latidos escuchando en {host}:{udp_port}")

    while not shutdown_event.is_set():
        try:
            data, addr = sock.recvfrom(1024)  # bloquea hasta 1 s
        except socket.timeout:
            continue  # sin paquetes: volvemos a revisar si hay que apagar
        except OSError:
            break     # socket cerrado durante el apagado

        mensaje = data.decode("utf-8", errors="ignore").strip()

        # Solo procesamos tramas de latido bien formadas.
        if not mensaje.startswith("PING:"):
            continue

        try:
            # PING:<node_id>|<cpu>|<status>
            cuerpo = mensaje[len("PING:"):]
            node_id, cpu, status = cuerpo.split("|")
        except ValueError:
            # Trama malformada: la ignoramos de forma robusta.
            continue

        ahora = time.time()
        with nodes_lock:
            ya_existia = node_id in nodes
            estaba_caido = ya_existia and not nodes[node_id]["alive"]
            nodes[node_id] = {
                "cpu": cpu,
                "status": status,
                "last_seen": ahora,     # marca de tiempo del ultimo latido
                "alive": True,
                "udp_addr": addr,
            }
        # Log de (re)aparicion de un nodo.
        if not ya_existia:
            print(f"[UDP] Nuevo Worker registrado: {node_id} desde {addr[0]}")
        elif estaba_caido:
            print(f"[UDP] Worker {node_id} volvio a estar ACTIVO")

        # Respondemos con un ACK al Worker (confirmacion del latido).
        try:
            sock.sendto(f"ACK:{node_id}".encode("utf-8"), addr)
        except OSError:
            pass  # si falla el ACK, el proximo latido lo reintenta

    sock.close()
    print("[UDP] Servidor de latidos detenido")


def monitor_nodos():
    """
    Hilo monitor: revisa periodicamente la tabla de nodos y marca como
    "Caido / Desconectado" a cualquier Worker que lleve mas de
    HEARTBEAT_TIMEOUT segundos sin enviar un latido.
    """
    while not shutdown_event.is_set():
        ahora = time.time()
        caidos = []
        with nodes_lock:
            for node_id, info in nodes.items():
                silencio = ahora - info["last_seen"]
                if info["alive"] and silencio > HEARTBEAT_TIMEOUT:
                    info["alive"] = False
                    info["status"] = "DOWN"
                    caidos.append((node_id, silencio))
        # Avisamos en consola (fuera del lock para no bloquear).
        for node_id, silencio in caidos:
            print(f"[MONITOR] Worker {node_id} declarado CAIDO "
                  f"(sin latidos por {silencio:.1f} s)")
        # Dormimos en tramos cortos para reaccionar rapido al apagado.
        shutdown_event.wait(MONITOR_INTERVAL)


# ===========================================================================
# ACTIVIDAD 2: Canal de comando y control (TCP multihilo)
# ===========================================================================
def leer_lineas(conn):
    """
    Generador que lee del socket TCP y entrega mensajes completos delimitados
    por DELIMITER ('\\n'). Acumula en un buffer porque TCP es un flujo de bytes
    y un recv() puede traer medio mensaje o varios juntos.
    """
    buffer = ""
    while not shutdown_event.is_set():
        try:
            datos = conn.recv(4096)
        except OSError:
            break
        if not datos:
            break  # el otro extremo cerro la conexion
        buffer += datos.decode("utf-8", errors="ignore")
        while DELIMITER in buffer:
            linea, buffer = buffer.split(DELIMITER, 1)
            if linea:
                yield linea


def manejar_worker_tcp(conn, addr):
    """
    Handler ejecutado en su propio hilo por cada Worker conectado.

    Flujo:
      1. El Worker, al conectarse, envia  REGISTER:<node_id>
      2. Guardamos la sesion en 'tcp_clients' para poder enviarle comandos.
      3. Escuchamos de forma continua sus informes (REPORT / STRESS_REPORT).
    """
    node_id = None
    try:
        for mensaje in leer_lineas(conn):
            if mensaje.startswith("REGISTER:"):
                node_id = mensaje[len("REGISTER:"):].strip()
                with tcp_lock:
                    tcp_clients[node_id] = {"conn": conn, "addr": addr}
                print(f"[TCP] Sesion persistente abierta con {node_id} "
                      f"({addr[0]}:{addr[1]})")

            elif mensaje.startswith("REPORT:"):
                # Informe de una tarea EXEC_TASK.
                # REPORT:<node_id>|<task>|<resultado>
                print(f"[TCP] Informe de tarea <- {mensaje[len('REPORT:'):]}")

            elif mensaje.startswith("STRESS_REPORT:"):
                # Informe de la rafaga sincronizada (Actividad 3).
                print(f"[TCP] Reporte de rafaga <- "
                      f"{mensaje[len('STRESS_REPORT:'):]}")

            else:
                print(f"[TCP] Mensaje no reconocido de {node_id}: {mensaje}")

    except Exception as e:
        print(f"[TCP] Error en la sesion con {node_id}: {e}")
    finally:
        # Limpieza de recursos al cerrarse la conexion (desconexion abrupta
        # o normal). Liberamos el socket y quitamos al nodo de la tabla TCP.
        if node_id:
            with tcp_lock:
                tcp_clients.pop(node_id, None)
            print(f"[TCP] Sesion cerrada con {node_id}")
        try:
            conn.close()
        except OSError:
            pass


def tcp_server(host, tcp_port):
    """
    Servidor TCP multihilo: acepta conexiones entrantes de Workers y lanza un
    hilo manejador por cada una, de modo que el Master atiende a muchos
    Workers simultaneamente sin bloquearse.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((host, tcp_port))
    sock.listen(16)          # cola de conexiones pendientes
    sock.settimeout(1.0)     # para revisar shutdown_event periodicamente
    print(f"[TCP] Canal de comando y control escuchando en {host}:{tcp_port}")

    while not shutdown_event.is_set():
        try:
            conn, addr = sock.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        # Un hilo dedicado por Worker (daemon para que no bloquee el apagado).
        hilo = threading.Thread(
            target=manejar_worker_tcp, args=(conn, addr), daemon=True)
        hilo.start()

    sock.close()
    print("[TCP] Servidor de comando y control detenido")


def enviar_a_worker(node_id, mensaje):
    """Envia un mensaje (ya con delimitador) a un Worker por su sesion TCP."""
    with tcp_lock:
        cliente = tcp_clients.get(node_id)
    if not cliente:
        print(f"[CLI] El Worker '{node_id}' no esta conectado por TCP")
        return False
    try:
        cliente["conn"].sendall(mensaje.encode("utf-8"))
        return True
    except OSError as e:
        print(f"[CLI] No se pudo enviar a {node_id}: {e}")
        return False


def destinatarios(objetivo):
    """
    Resuelve el destino de un comando de la CLI.
      - 'all'      -> todos los Workers con sesion TCP activa.
      - <node_id>  -> ese Worker en concreto.
    """
    with tcp_lock:
        if objetivo == "all":
            return list(tcp_clients.keys())
        return [objetivo] if objetivo in tcp_clients else []


# ===========================================================================
# ACTIVIDAD 3 (apoyo): servidor objetivo simulado local
# ===========================================================================
def servidor_objetivo(port):
    """
    Pequeno servidor TCP de prueba que actua como OBJETIVO SIMULADO para la
    rafaga de carga. Acepta conexiones, lee lo que llegue y responde 'OK'.
    Sirve para generar evidencias del laboratorio de forma autocontenida,
    sin tocar ningun servicio real de terceros.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", port))   # SOLO loopback: objetivo local
    sock.listen(64)
    sock.settimeout(1.0)
    print(f"[TARGET] Objetivo simulado escuchando en 127.0.0.1:{port}")

    def atender(c):
        try:
            c.recv(1024)
            c.sendall(b"OK")
        except OSError:
            pass
        finally:
            c.close()

    while not shutdown_event.is_set():
        try:
            c, _ = sock.accept()
        except socket.timeout:
            continue
        except OSError:
            break
        threading.Thread(target=atender, args=(c,), daemon=True).start()
    sock.close()


# ===========================================================================
# CLI de control (hilo principal)
# ===========================================================================
AYUDA = """
Comandos disponibles:
  list                                  Muestra la tabla de nodos (UDP) y sesiones TCP.
  exec <node_id|all> <tarea>            Envia una tarea a un Worker o a todos.
                                        Ej: exec all scan_ports
  stress <node_id|all> <host> <port> <seg>
                                        Lanza la rafaga sincronizada contra un
                                        objetivo LOCAL simulado por <seg> segundos.
                                        Ej: stress all 127.0.0.1 7000 5
  target <port>                         Levanta un objetivo simulado local (loopback).
  help                                  Muestra esta ayuda.
  quit                                  Apaga el Master ordenadamente.
"""


def imprimir_tabla():
    """Muestra el inventario de nodos con su estado actual."""
    ahora = time.time()
    print("\n--- TABLA DE NODOS (latidos UDP) ---")
    print(f"{'NODE_ID':<12}{'ESTADO':<10}{'CPU':<8}{'ULT.LATIDO':<12}{'TCP':<6}")
    with nodes_lock, tcp_lock:
        if not nodes:
            print("  (sin nodos registrados todavia)")
        for node_id, info in nodes.items():
            estado = "ACTIVO" if info["alive"] else "CAIDO"
            hace = f"{ahora - info['last_seen']:.1f}s"
            tiene_tcp = "si" if node_id in tcp_clients else "no"
            print(f"{node_id:<12}{estado:<10}{info['cpu']:<8}{hace:<12}{tiene_tcp:<6}")
    print("-------------------------------------\n")


def cli_control():
    """Bucle de la interfaz de control del operador."""
    print(AYUDA)
    while not shutdown_event.is_set():
        try:
            linea = input("master> ").strip()
        except (EOFError, KeyboardInterrupt):
            linea = "quit"

        if not linea:
            continue
        partes = linea.split()
        cmd = partes[0].lower()

        if cmd == "help":
            print(AYUDA)

        elif cmd == "list":
            imprimir_tabla()

        elif cmd == "exec":
            # exec <node_id|all> <tarea>
            if len(partes) < 3:
                print("Uso: exec <node_id|all> <tarea>")
                continue
            objetivo, tarea = partes[1], partes[2]
            dests = destinatarios(objetivo)
            if not dests:
                print(f"No hay destinatarios para '{objetivo}'")
                continue
            for node_id in dests:
                enviar_a_worker(node_id, f"EXEC_TASK:{tarea}{DELIMITER}")
            print(f"[CLI] Tarea '{tarea}' enviada a: {', '.join(dests)}")

        elif cmd == "stress":
            # stress <node_id|all> <host> <port> <seg>
            if len(partes) < 5:
                print("Uso: stress <node_id|all> <host> <port> <seg>")
                continue
            objetivo, host, port, seg = partes[1], partes[2], partes[3], partes[4]
            dests = destinatarios(objetivo)
            if not dests:
                print(f"No hay destinatarios para '{objetivo}'")
                continue
            # Enviamos la orden a todos "a la vez": recorremos la lista y
            # despachamos el mismo comando a cada sesion TCP abierta.
            orden = f"BROADCAST_STRESS:{host}:{port}:{seg}{DELIMITER}"
            for node_id in dests:
                enviar_a_worker(node_id, orden)
            print(f"[CLI] Rafaga sincronizada enviada a: {', '.join(dests)} "
                  f"(objetivo {host}:{port}, {seg}s)")

        elif cmd == "target":
            # target <port>
            if len(partes) < 2:
                print("Uso: target <port>")
                continue
            try:
                port = int(partes[1])
            except ValueError:
                print("El puerto debe ser un numero")
                continue
            threading.Thread(
                target=servidor_objetivo, args=(port,), daemon=True).start()

        elif cmd == "quit":
            print("[CLI] Apagando el Master...")
            shutdown_event.set()
            break

        else:
            print(f"Comando desconocido: {cmd}. Escribe 'help'.")


# ===========================================================================
# Arranque
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="Master - Taller de Sockets")
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--tcp-port", type=int, default=DEFAULT_TCP_PORT)
    parser.add_argument("--udp-port", type=int, default=DEFAULT_UDP_PORT)
    args = parser.parse_args()

    print("=== MASTER - Nodo Central de Comando y Control ===")

    # Lanzamos los servidores y el monitor como hilos daemon.
    hilos = [
        threading.Thread(target=udp_server,
                         args=(args.host, args.udp_port), daemon=True),
        threading.Thread(target=tcp_server,
                         args=(args.host, args.tcp_port), daemon=True),
        threading.Thread(target=monitor_nodos, daemon=True),
    ]
    for h in hilos:
        h.start()

    # La CLI corre en el hilo principal (bloquea hasta 'quit').
    try:
        cli_control()
    finally:
        shutdown_event.set()
        time.sleep(1.2)  # damos tiempo a que los hilos cierren sus sockets
        print("Master finalizado. Recursos liberados.")


if __name__ == "__main__":
    main()
