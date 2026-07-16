# ============================================================
#  SERVIDOR - Tic Tac Toe 3D en Red (4x4x4)
#  Arbitro del juego: valida turnos, casillas y detecta ganador.
#  Se comunica por WebSockets para poder correr en un servidor
#  gratuito en la nube (Render.com) y que los 2 jugadores se
#  conecten desde cualquier parte a una URL fija.
# ============================================================

import asyncio
import json
import os
import websockets

PUERTO = int(os.environ.get('PORT', 5555))

# ------------------------------------------------------------------
# Logica de juego (igual a la del programa original del profesor:
# matriz jugadas[z][y][x] y las 13 combinaciones ganadoras C[])
# ------------------------------------------------------------------
def tablero_vacio():
    return [[[0] * 4 for _ in range(4)] for _ in range(4)]

jugadas = tablero_vacio()
turno = 0          # 0 = turno de "X", 1 = turno de "O"
terminado = False

C = [[1, 1, 0], [1, 0, 1], [0, 1, 1], [1, 0, 0], [1, -1, 0], [0, 0, 1],
     [-1, 0, 1], [0, 1, 0], [0, 1, -1], [0, -1, -1], [0, -1, 0],
     [0, 0, -1], [0, 0, 0]]

def indice_a_xyz(i):
    return i % 4, (i % 16) // 4, i // 16   # x, y, z

def revisar_linea(c, X, Y, Z):
    tz, ty, tx = C[c]
    z1 = Z if tz > 0 else -1
    y1 = Y if ty > 0 else -1
    x1 = X if tx > 0 else -1
    s, celdas = 0, []
    for i in range(4):
        z = Z if z1 >= 0 else (3 - i if tz else i)
        y = Y if y1 >= 0 else (3 - i if ty else i)
        x = X if x1 >= 0 else (3 - i if tx else i)
        s += jugadas[z][y][x]
        celdas.append(z * 16 + y * 4 + x)
    return celdas if s in (4, -4) else None

def hay_ganador(X, Y, Z):
    for c in range(13):
        linea = revisar_linea(c, X, Y, Z)
        if linea:
            return linea
    return None

# ------------------------------------------------------------------
# Red: manejo de conexiones y mensajes (JSON por WebSocket)
# ------------------------------------------------------------------
clientes = {}   # websocket -> jugador_id (0 o 1)
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
    # Responde OK a peticiones HTTP normales (health checks de Render);
    # solo deja pasar conexiones que sí son WebSocket de verdad.
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

    await enviar(websocket, {'tipo': 'bienvenida', 'jugador': jugador_id})
    if len(clientes) < 2:
        await enviar(websocket, {'tipo': 'esperando'})
    else:
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

async def main():
    async with websockets.serve(manejador, '0.0.0.0', PUERTO, process_request=process_request):
        print(f'Servidor escuchando en el puerto {PUERTO}...')
        await asyncio.Future()

if __name__ == '__main__':
    asyncio.run(main())

