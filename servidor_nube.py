# ============================================================
#  SERVIDOR - Tic Tac Toe 3D en Red (4x4x4) - version NUBE
# ============================================================
#  Este es el mismo arbitro que la version anterior, pero en vez
#  de sockets "crudos" (TCP) usa WEBSOCKETS. La razon es que los
#  servidores gratis en internet (Render, Railway, etc.) solo dejan
#  pasar trafico HTTP/WebSocket, no TCP puro. Con WebSockets el
#  servidor se puede publicar en internet y los 2 jugadores se
#  conectan a una URL fija (https://tu-app.onrender.com), sin
#  importar en que ciudad o pais este cada uno.
#
#  La logica del juego es EXACTAMENTE la misma que en servidor.py
#  (matriz jugadas[z][y][x] y las 13 lineas ganadoras C[]).
#
#  Protocolo de mensajes: JSON (un mensaje = un envio de WebSocket)
#
#   Cliente -> Servidor:
#     {"tipo":"click", "i": <0-63>}
#     {"tipo":"reiniciar"}
#
#   Servidor -> Cliente:
#     {"tipo":"bienvenida", "jugador": 0|1}
#     {"tipo":"esperando"}
#     {"tipo":"inicio", "turno": 0}
#     {"tipo":"jugada", "i":.., "jugador":.., "marca":"X"|"O"}
#     {"tipo":"turno", "turno": 0|1}
#     {"tipo":"invalida", "motivo": "..."}
#     {"tipo":"gano", "jugador":.., "linea":[i1,i2,i3,i4]}
#     {"tipo":"reinicio"}
#     {"tipo":"rival_desconectado"}
#
#  Como correrlo en LOCAL para pruebas:
#     python servidor.py
#  Como lo corre Render.com en la nube (automatico, ver tutorial):
#     Render define la variable de entorno PORT y nosotros la leemos.
# ============================================================

import asyncio
import json
import os
import websockets

PUERTO = int(os.environ.get('PORT', 5555))

# ------------------------------------------------------------------
# Logica de juego (identica a la version original del profesor)
# ------------------------------------------------------------------
def tablero_vacio():
    return [[[0] * 4 for _ in range(4)] for _ in range(4)]

jugadas = tablero_vacio()
turno = 0          # 0 = turno del jugador "X", 1 = turno del jugador "O"
terminado = False

#  C[1,0,-1]  ->  1 = No Varia, 0 = Varia 0,1,2,3, -1 = Varia 3,2,1,0
C = [[1, 1, 0], [1, 0, 1], [0, 1, 1], [1, 0, 0], [1, -1, 0], [0, 0, 1],
     [-1, 0, 1], [0, 1, 0], [0, 1, -1], [0, -1, -1], [0, -1, 0],
     [0, 0, -1], [0, 0, 0]]

def indice_a_xyz(i):
    z = i // 16
    y = (i % 16) // 4
    x = i % 4
    return x, y, z

def revisar_linea(c, X, Y, Z):
    tz, ty, tx = C[c]
    z1 = Z if tz > 0 else -1
    y1 = Y if ty > 0 else -1
    x1 = X if tx > 0 else -1
    s = 0
    celdas = []
    for i in range(4):
        z = Z if z1 >= 0 else (3 - i if tz else i)
        y = Y if y1 >= 0 else (3 - i if ty else i)
        x = X if x1 >= 0 else (3 - i if tx else i)
        s += jugadas[z][y][x]
        celdas.append(z * 16 + y * 4 + x)
    if s == 4 or s == -4:
        return celdas
    return None

def hay_ganador(X, Y, Z):
    for c in range(13):
        linea = revisar_linea(c, X, Y, Z)
        if linea:
            return linea
    return None

# ------------------------------------------------------------------
# Manejo de red (asyncio + websockets)
# ------------------------------------------------------------------
clientes = {}                       # websocket -> jugador_id (0 o 1)
lock = asyncio.Lock()

async def enviar(ws, obj):
    try:
        await ws.send(json.dumps(obj))
    except Exception:
        pass

async def enviar_a_todos(obj):
    for ws in list(clientes.keys()):
        await enviar(ws, obj)

async def process_request(connection, request):
    """Render (y cualquier navegador) hacen de vez en cuando una peticion
       HTTP normal (no un 'upgrade' a WebSocket) para revisar que el
       servicio siga vivo (health check). Sin este manejo, la libreria
       websockets marcaba eso como error (EOFError / InvalidMessage) en
       los logs, aunque el juego funcionara bien. Aqui respondemos un
       OK simple a esas peticiones y dejamos pasar solo las conexiones
       reales de WebSocket (las que sí traen el header 'Upgrade')."""
    if request.headers.get('Upgrade', '').lower() != 'websocket':
        return connection.respond(200, 'Servidor Tic Tac Toe 3D activo.\n')
    return None

async def manejador(websocket):
    global turno, terminado, jugadas

    async with lock:
        if len(clientes) >= 2:
            await enviar(websocket, {'tipo': 'invalida', 'motivo': 'Ya hay 2 jugadores conectados'})
            await websocket.close()
            return
        jugador_id = len(clientes)
        clientes[websocket] = jugador_id

    print(f'Jugador {jugador_id} conectado')
    await enviar(websocket, {'tipo': 'bienvenida', 'jugador': jugador_id})
    if len(clientes) < 2:
        await enviar(websocket, {'tipo': 'esperando'})
    else:
        print('Los 2 jugadores estan listos. Empieza el juego.')
        await enviar_a_todos({'tipo': 'inicio', 'turno': turno})

    try:
        async for mensaje in websocket:
            msg = json.loads(mensaje)
            tipo = msg.get('tipo')

            if tipo == 'click':
                async with lock:
                    if terminado:
                        await enviar(websocket, {'tipo': 'invalida', 'motivo': 'Partida terminada'})
                        continue
                    if jugador_id != turno:
                        await enviar(websocket, {'tipo': 'invalida', 'motivo': 'No es tu turno'})
                        continue
                    i = msg['i']
                    x, y, z = indice_a_xyz(i)
                    if jugadas[z][y][x] != 0:
                        await enviar(websocket, {'tipo': 'invalida', 'motivo': 'Casilla ocupada'})
                        continue
                    jugadas[z][y][x] = -1 if jugador_id == 0 else 1
                    marca = 'X' if jugador_id == 0 else 'O'
                    await enviar_a_todos({'tipo': 'jugada', 'i': i, 'jugador': jugador_id, 'marca': marca})
                    linea_ganadora = hay_ganador(x, y, z)
                    if linea_ganadora:
                        terminado = True
                        await enviar_a_todos({'tipo': 'gano', 'jugador': jugador_id, 'linea': linea_ganadora})
                    else:
                        turno = 0 if turno == 1 else 1
                        await enviar_a_todos({'tipo': 'turno', 'turno': turno})

            elif tipo == 'reiniciar':
                async with lock:
                    jugadas = tablero_vacio()
                    turno = 0
                    terminado = False
                await enviar_a_todos({'tipo': 'reinicio'})
                await enviar_a_todos({'tipo': 'turno', 'turno': turno})

    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        async with lock:
            clientes.pop(websocket, None)
        await enviar_a_todos({'tipo': 'rival_desconectado'})
        print(f'Jugador {jugador_id} desconectado')

async def main():
    async with websockets.serve(manejador, '0.0.0.0', PUERTO, process_request=process_request):
        print(f'Servidor de Tic Tac Toe 3D escuchando en el puerto {PUERTO}...')
        await asyncio.Future()  # corre para siempre

if __name__ == '__main__':
    asyncio.run(main())
