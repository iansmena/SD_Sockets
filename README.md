# Taller de Sockets — Arquitectura Master / Worker

Sistemas Distribuidos — Universidad Politécnica Salesiana

Infraestructura **Master–Worker** en Python puro (sin dependencias externas)
que implementa:

1. **Canal de latidos y registro (UDP)** — inventario dinámico de nodos con
   detección automática de caídas.
2. **Canal de comando y control (TCP multihilo)** — sesión persistente por
   Worker y despacho de comandos en tiempo real.
3. **Ráfaga sincronizada** — orden global desde el Master y simulación de
   carga concurrente con recolección de métricas.

> **Alcance de la Actividad 3.** La "ráfaga de estrés" se implementa como una
> **simulación de carga controlada contra un objetivo local** (loopback o red
> privada RFC 1918), que es el escenario del entregable ("máquina local o red
> virtual"). El Worker **rechaza** cualquier objetivo que sea una dirección
> pública. El Master incluye un objetivo simulado para generar las evidencias
> de forma autocontenida.

---

## 1. Requisitos

- Python 3.8 o superior. No requiere instalar nada.
- (Opcional) `psutil` para que el Worker reporte el uso **real** de CPU en el
  latido; si no está instalado, se simula un valor:
  ```bash
  pip install psutil   # opcional
  ```

---

## 2. Instrucciones de ejecución

Ejecutar siempre **primero el Master** y luego los Workers. Cada proceso va en
su propia terminal.

### Paso 1 — Levantar el Master

```bash
python3 master.py
# o especificando host/puertos:
python3 master.py --host 0.0.0.0 --tcp-port 6000 --udp-port 6001
```

El Master abre una **CLI de control** (`master>`). Comandos disponibles:

| Comando | Descripción |
|---|---|
| `list` | Muestra la tabla de nodos (estado, CPU, último latido, sesión TCP). |
| `exec <node_id\|all> <tarea>` | Envía una tarea a un Worker o a todos. Ej: `exec all scan_ports`. |
| `stress <node_id\|all> <host> <port> <seg>` | Lanza la ráfaga sincronizada contra un objetivo local. Ej: `stress all 127.0.0.1 7000 5`. |
| `target <port>` | Levanta un objetivo simulado local (solo loopback). |
| `help` | Muestra la ayuda. |
| `quit` | Apaga el Master y libera los puertos. |

### Paso 2 — Levantar el objetivo simulado (para la Actividad 3)

En la CLI del Master:

```
master> target 7000
```

### Paso 3 — Conectar varios Workers

En otras terminales (misma máquina o red local):

```bash
python3 worker.py --id worker-1 --master 127.0.0.1
python3 worker.py --id worker-2 --master 127.0.0.1
# En otra máquina de la red virtual:
python3 worker.py --id worker-3 --master 192.168.1.50
```

Parámetros del Worker:

| Parámetro | Por defecto | Descripción |
|---|---|---|
| `--id` | (obligatorio) | Identificador único del nodo. |
| `--master` | `127.0.0.1` | Host/IP del Master. |
| `--tcp-port` | `6000` | Puerto TCP de comando y control. |
| `--udp-port` | `6001` | Puerto UDP de latidos. |

### Paso 4 — Operar

```
master> list
master> exec all scan_ports
master> stress all 127.0.0.1 7000 5
master> list
```

Para probar la detección de caídas, cierra un Worker con `Ctrl+C` (o mátalo) y
espera ~6 segundos: el Master lo marcará como **CAIDO**.

---

## 3. Descripción del protocolo

### 3.1 Canal UDP — Latidos (Actividad 1)

No orientado a conexión. Cada Worker envía un datagrama cada **2 s**.

| Sentido | Trama | Ejemplo |
|---|---|---|
| Worker → Master | `PING:<node_id>\|<cpu_usage>\|<status>` | `PING:worker-1\|43%\|ALIVE` |
| Master → Worker | `ACK:<node_id>` | `ACK:worker-1` |

- Separador de campos: `|` (barra vertical).
- El Master guarda un **timestamp** (`last_seen`) por nodo.
- Regla de caída: si `ahora - last_seen > 6 s`, el nodo pasa a **DOWN**.

### 3.2 Canal TCP — Comando y Control (Actividades 2 y 3)

Orientado a conexión, persistente y multihilo. **Delimitador de mensajes:**
salto de línea `\n` (cada mensaje termina en `\n`).

**Al conectarse, el Worker se registra:**

| Sentido | Trama | Ejemplo |
|---|---|---|
| Worker → Master | `REGISTER:<node_id>` | `REGISTER:worker-1` |

**Comandos (Master → Worker):**

| Trama | Significado |
|---|---|
| `EXEC_TASK:<tarea>` | Ejecuta una tarea en segundo plano. Tareas: `scan_ports`, `fetch_logs`, u otra (eco). |
| `BROADCAST_STRESS:<host>:<port>:<duracion>` | Inicia la ráfaga de carga contra el objetivo local por `<duracion>` s. |

**Informes (Worker → Master):**

| Trama | Significado |
|---|---|
| `REPORT:<node_id>\|<tarea>\|<resultado>` | Resultado de una `EXEC_TASK`. |
| `STRESS_REPORT:<node_id>\|requests=<n>\|avg_ms=<x>\|errors=<e>` | Métricas de la ráfaga. |
| `STRESS_REPORT:<node_id>\|RECHAZADO\|motivo=...` | El objetivo no era local/privado. |

### 3.3 Diagrama de flujo

```
         UDP :6001  (latidos cada 2s)
 Worker ───────────────────────────────▶ Master
   │   ◀───────────── ACK ──────────────   │  (tabla de nodos + timeouts)
   │                                        │
   │        TCP :6000  (persistente)        │
   │ ── REGISTER:worker-1 ────────────────▶ │
   │ ◀── EXEC_TASK:scan_ports ───────────── │  (hilo por Worker)
   │ ── REPORT:worker-1|scan_ports|... ───▶ │
   │ ◀── BROADCAST_STRESS:127.0.0.1:7000:5  │
   │ ── STRESS_REPORT:worker-1|requests=... ▶│
```

---

## 4. Evidencias de funcionamiento

Para el informe se recomienda capturar:

1. **Registro UDP:** salida del Master mostrando `Nuevo Worker registrado`
   para cada nodo y la tabla con `list`.
2. **Caída automática:** cerrar un Worker y capturar
   `[MONITOR] Worker worker-X declarado CAIDO` tras ~6 s, más la tabla con el
   estado `CAIDO`.
3. **Comando en vivo (TCP):** `exec all scan_ports` y los `REPORT:` recibidos.
4. **Ráfaga sincronizada:** `stress all 127.0.0.1 7000 5` y los
   `STRESS_REPORT:` con las métricas de cada Worker.

---

## 5. Manejo de errores y limpieza de recursos

- Todos los sockets usan `SO_REUSEADDR` para evitar "Address already in use".
- Los `recv`/`recvfrom` están protegidos con `try/except` y timeouts.
- Las desconexiones abruptas de Workers se detectan y limpian de las tablas.
- El Worker **reintenta** la conexión TCP si se pierde (tolerancia a fallos).
- `quit` / `Ctrl+C` apagan ordenadamente y cierran todos los sockets.

---

## 6. Estructura de archivos

```
taller_sockets/
├── master.py    # Nodo central: servidores UDP y TCP + CLI de control
├── worker.py    # Nodo de trabajo: latidos, sesión TCP, tareas y ráfaga
└── README.md    # Este documento
```
