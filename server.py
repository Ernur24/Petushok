import asyncio
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI()


# --- КЛАССЫ ИГРЫ ---
class Card:
    def __init__(self, suit, value, rank):
        self.suit = suit
        self.value = value
        self.rank = rank

    def to_dict(self):
        return {"suit": self.suit, "value": self.value, "rank": self.rank, "str": f"{self.value}{self.suit}"}


class Deck:
    def __init__(self):
        suits = ['♥️', '♦️', '♣️', '♠️']
        values = [('6', 6), ('7', 7), ('8', 8), ('9', 9), ('10', 10), ('J', 11), ('Q', 12), ('K', 13), ('A', 14)]
        self.cards = [Card(s, v, r) for s in suits for v, r in values]
        random.shuffle(self.cards)

    def draw(self, count):
        drawn = self.cards[:count]
        self.cards = self.cards[count:]
        return drawn


class GameEngine:
    def __init__(self):
        self.players = []  # Список игроков
        self.status = "waiting"  # waiting, myrat, declaration, playing, ended
        self.trump_card = None
        self.myrat = []
        self.current_leader_id = None  # Кто взял прошлую взятку - ходит первым
        self.current_turn_idx = 0
        self.lead_suit = None
        self.current_trick = []  # Карты на столе [(player_id, card)]
        self.tricks_played = 0

    def start_round(self):
        deck = Deck()
        self.current_trick = []
        self.lead_suit = None
        self.tricks_played = 0
        self.myrat = [c.to_dict() for c in deck.draw(4)]
        self.trump_card = deck.draw(1)[0].to_dict()

        for p in self.players:
            p['hand'] = [c.to_dict() for c in deck.draw(5)]
            p['is_folded'] = False
            p['tricks_won'] = 0

        if self.current_leader_id is None:
            self.current_leader_id = self.players[0]['id']

        self.status = "myrat"
        self.current_turn_idx = 0


# --- МЕНЕДЖЕР СОЕДИНЕНИЙ ---
class ConnectionManager:
    def __init__(self):
        self.active_connections = {}

    async def connect(self, websocket: WebSocket, client_id: str):
        await websocket.accept()
        self.active_connections[client_id] = websocket

    def disconnect(self, client_id: str):
        if client_id in self.active_connections:
            del self.active_connections[client_id]

    async def broadcast(self, message: dict):
        for connection in self.active_connections.values():
            await connection.send_json(message)


manager = ConnectionManager()
game = GameEngine()


def get_game_state(client_id):
    """Формирует состояние игры индивидуально для каждого игрока (скрывая чужие карты)"""
    active_players_count = sum(1 for p in game.players if not p.get('is_folded', False))

    players_clean = []
    for p in game.players:
        players_clean.append({
            "id": p["id"],
            "name": p["name"],
            "score": p["score"],
            "is_folded": p.get("is_folded", False),
            "tricks_won": p.get("tricks_won", 0),
            "cards_count": len(p["hand"]),
            "is_current": game.players[game.current_turn_idx]["id"] == p["id"] if game.status != "waiting" else False
        })

    my_data = next((p for p in game.players if p["id"] == client_id), None)

    # Новое правило: Если осталось всего 2 активных игрока, пас запрещен
    can_fold = active_players_count > 2

    return {
        "status": game.status,
        "trump": game.trump_card,
        "players": players_clean,
        "current_trick": [{"player_name": next(p["name"] for p in game.players if p["id"] == tid), "card": c} for tid, c
                          in game.current_trick],
        "lead_suit": game.lead_suit,
        "my_hand": my_data["hand"] if my_data else [],
        "can_fold": can_fold
    }


async def update_all_clients():
    for cid in manager.active_connections.keys():
        try:
            await manager.active_connections[cid].send_json({"type": "state", "data": get_game_state(cid)})
        except:
            pass


@app.websocket("/ws/{client_id}/{player_name}")
async def websocket_endpoint(websocket: WebSocket, client_id: str, player_name: str):
    await manager.connect(websocket, client_id)

    # Регистрация игрока, если его нет
    if not any(p['id'] == client_id for p in game.players) and game.status == "waiting":
        game.players.append({"id": client_id, "name": player_name, "score": 15, "hand": []})

    await update_all_clients()

    try:
        while True:
            data = await websocket.receive_json()
            current_player = game.players[game.current_turn_idx] if game.status != "waiting" else None

            # Старт игры
            if data["type"] == "START_GAME" and game.status == "waiting":
                if len(game.players) >= 2:
                    game.start_round()

            # Действие в фазе Мырата
            elif data["type"] == "MYRAT_DECISION" and game.status == "myrat" and current_player["id"] == client_id:
                if data["action"] == "take":
                    current_player["hand"] = current_player["hand"][4:] + game.myrat
                    game.myrat = []
                    game.status = "declaration"
                    game.current_turn_idx = 0
                else:
                    game.current_turn_idx += 1
                    if game.current_turn_idx >= len(game.players):
                        game.myrat = []
                        game.status = "declaration"
                        game.current_turn_idx = 0

            # Действие в фазе Паса (Заявки)
            elif data["type"] == "DECLARATION_DECISION" and game.status == "declaration" and current_player[
                "id"] == client_id:
                active_count = sum(1 for p in game.players if not p.get('is_folded', False))

                if data["action"] == "fold" and active_count > 2:
                    current_player["is_folded"] = True
                else:
                    current_player["is_folded"] = False

                game.current_turn_idx += 1
                if game.current_turn_idx >= len(game.players):
                    # Переход к игре. Ищем лидера раунда
                    while True:
                        leader_p = next(p for p in game.players if p['id'] == game.current_leader_id)
                        if not leader_p.get('is_folded', False):
                            break
                        idx = game.players.index(leader_p)
                        game.current_leader_id = game.players[(idx + 1) % len(game.players)]['id']

                    game.status = "playing"
                    game.current_turn_idx = game.players.index(leader_p)

            # Ход картой
            elif data["type"] == "PLAY_CARD" and game.status == "playing" and current_player["id"] == client_id:
                card_idx = data["card_idx"]
                card = current_player["hand"].pop(card_idx)

                if game.lead_suit is None:
                    game.lead_suit = card["suit"]

                game.current_trick.append((client_id, card))

                # Переход хода к следующему активному
                active_round_players = [p for p in game.players if not p.get('is_folded', False)]
                if len(game.current_trick) < len(active_round_players):
                    while True:
                        game.current_turn_idx = (game.current_turn_idx + 1) % len(game.players)
                        if not game.players[game.current_turn_idx].get('is_folded', False):
                            break
                else:
                    # Подсчет взятки
                    await asyncio.sleep(2)  # Даем посмотреть карты на столе
                    trump_suit = game.trump_card["suit"]
                    w_id, b_card = game.current_trick[0]
                    for pid, c in game.current_trick[1:]:
                        if c["suit"] == trump_suit:
                            if b_card["suit"] != trump_suit or c["rank"] > b_card["rank"]:
                                w_id, b_card = pid, c
                        elif c["suit"] == game.lead_suit and b_card["suit"] != trump_suit:
                            if c["rank"] > b_card["rank"]:
                                w_id, b_card = pid, c

                    winner_p = next(p for p in game.players if p["id"] == w_id)
                    winner_p["tricks_won"] += 1
                    game.current_leader_id = w_id  # Твое правило: победитель взятки ходит следующим!
                    game.current_turn_idx = game.players.index(winner_p)
                    game.current_trick = []
                    game.lead_suit = None
                    game.tricks_played += 1

                    if game.tricks_played >= 5:
                        # Конец раунда, подсчет очков
                        for p in game.players:
                            if p.get('is_folded', False): continue
                            if p["tricks_won"] == 0:
                                p["score"] += 5
                            else:
                                p["score"] -= p["won"]
                        game.status = "waiting"

            await update_all_clients()
    except WebSocketDisconnect:
        manager.disconnect(client_id)
        await update_all_clients()


app.mount("/", StaticFiles(directory="public", html=True), name="public")