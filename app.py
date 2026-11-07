"""
Tutorial 11: Dashboard IoT en Streamlit con MQTT y Broker Mosquitto
===================================================================
Aprende a integrar el protocolo MQTT en Streamlit para monitorear telemetría
de alta frecuencia y publicar órdenes de control a microcontroladores ESP32.
"""

from collections import deque
import json
import random
import time
from typing import Dict, List, Optional
import paho.mqtt.client as mqtt
import pandas as pd
import streamlit as st

st.set_page_config(
    page_title="Tutorial 11 - Dashboard IoT MQTT",
    page_icon="📡",
    layout="wide"
)

st.title("📡 Tutorial 11: Dashboard IoT con MQTT y Mosquitto")
st.write(
    "MQTT (Message Queuing Telemetry Transport) es el estándar indiscutible de mensajería ligera "
    "para IoT. Su modelo de **Publicador / Suscriptor** a través de un intermediario (*Broker*) "
    "permite transmitir telemetría con mínima sobrecarga de red y baja latencia."
)

st.divider()

# =====================================================================
# GESTOR PERSISTENTE DE CLIENTE MQTT
# =====================================================================
class StreamlitMQTTManager:
    """Mantiene una conexión persistente en segundo plano con el Broker MQTT."""
    def __init__(self, broker: str, port: int, client_id_prefix: str = "st_client"):
        self.broker = broker
        self.port = port
        self.client_id = f"{client_id_prefix}_{random.randint(1000, 9999)}"
        self.connected = False
        self.history: deque = deque(maxlen=50)
        self.logs: deque = deque(maxlen=25)
        
        # Cliente MQTT con la versión de API más moderna de paho-mqtt
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.client_id)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.connect()

    def _log(self, text: str):
        t = time.strftime("%H:%M:%S")
        self.logs.append(f"[{t}] {text}")

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        if rc == 0:
            self.connected = True
            self._log(f"🟢 Conectado con éxito a {self.broker}:{self.port}")
        else:
            self.connected = False
            self._log(f"🔴 Fallo de conexión (Código: {rc})")

    def _on_disconnect(self, client, userdata, disconnect_flags, rc=None, properties=None):
        self.connected = False
        self._log("🔌 Desconectado del Broker.")

    def _on_message(self, client, userdata, msg):
        try:
            payload_str = msg.payload.decode("utf-8")
            self._log(f"RX en [{msg.topic}]: {payload_str}")
            data = json.loads(payload_str)
            data["topic"] = msg.topic
            if "timestamp" not in data:
                data["timestamp"] = time.strftime("%H:%M:%S")
            self.history.append(data)
        except Exception as e:
            self._log(f"⚠️ Error al decodificar payload: {e}")

    def connect(self):
        try:
            self.client.connect(self.broker, self.port, keepalive=60)
            self.client.loop_start()  # Hilo en segundo plano para procesar paquetes
            self._log(f"Iniciando bucle de red hacia {self.broker}...")
        except Exception as e:
            self._log(f"❌ Error al iniciar conexión: {e}")

    def subscribe(self, topic: str):
        if self.client:
            self.client.subscribe(topic, qos=0)
            self._log(f"📥 Suscrito al tópico: {topic}")

    def publish(self, topic: str, payload: dict) -> bool:
        if not self.client:
            return False
        try:
            payload_str = json.dumps(payload)
            res = self.client.publish(topic, payload_str, qos=1)
            self._log(f"TX hacia [{topic}]: {payload_str}")
            return res.rc == mqtt.MQTT_ERR_SUCCESS
        except Exception as e:
            self._log(f"❌ Error en publicación: {e}")
            return False

    def disconnect(self):
        try:
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False
        except Exception:
            pass

# Guardamos el cliente en caché de recursos para que persista entre reruns
@st.cache_resource
def get_mqtt_manager(broker: str, port: int) -> StreamlitMQTTManager:
    return StreamlitMQTTManager(broker, port)

# =====================================================================
# BARRA LATERAL: CONFIGURACIÓN DEL BROKER
# =====================================================================
with st.sidebar:
    st.header("⚙️ Configuración del Broker MQTT")
    
    opcion_broker = st.selectbox(
        "Seleccionar Servidor Broker:",
        options=["broker.emqx.io (Público de Pruebas)", "test.mosquitto.org (Público)", "localhost (Mosquitto Local)"]
    )
    
    if "emqx" in opcion_broker:
        broker_host = "broker.emqx.io"
    elif "mosquitto.org" in opcion_broker:
        broker_host = "test.mosquitto.org"
    else:
        broker_host = "localhost"
        
    broker_port = st.number_input("Puerto:", value=1883, step=1)
    device_id = st.text_input("ID del Dispositivo ESP32:", value="ESP32_MQTT_01")
    
    topic_sub = f"iot/sensores/{device_id}/telemetria"
    topic_pub = f"iot/sensores/{device_id}/comandos"

    st.caption(f"**Tópico Suscrito (RX):** `{topic_sub}`")
    st.caption(f"**Tópico Publicación (TX):** `{topic_pub}`")

    col_btn_m1, col_btn_m2 = st.columns(2)
    with col_btn_m1:
        if st.button("🔌 Reconectar", use_container_width=True):
            st.cache_resource.clear()
            st.rerun()

# Obtener instancia persistente
mqtt_mgr = get_mqtt_manager(broker_host, int(broker_port))
# Asegurar suscripción
mqtt_mgr.subscribe(topic_sub)

# Estado de la conexión
if mqtt_mgr.connected:
    st.success(f"🟢 **Conectado al Broker:** `{broker_host}:{broker_port}` | Escuchando telemetría de `{device_id}`")
else:
    st.warning(f"🟡 Intentando conectar al Broker `{broker_host}:{broker_port}`... Si usas localhost, verifica que Mosquitto esté corriendo.")

st.divider()

# =====================================================================
# PESTAÑAS DEL DASHBOARD
# =====================================================================
tab_dash, tab_control, tab_terminal, tab_mosquitto_guide = st.tabs([
    "📊 Telemetría en Tiempo Real",
    "🎛️ Control de Actuadores (Publish)",
    "📜 Terminal de Mensajes MQTT",
    "📖 Guía de Mosquitto y CLI"
])

# -------------------------------------------------------------
# TAB 1: TELEMETRÍA
# -------------------------------------------------------------
with tab_dash:
    col_t_btn1, col_t_btn2 = st.columns([3, 1])
    with col_t_btn1:
        st.subheader("Variables Ambientales Recibidas")
    with col_t_btn2:
        if st.button("🔄 Actualizar Vista", use_container_width=True, type="primary"):
            st.rerun()

    lista_datos = list(mqtt_mgr.history)
    
    if lista_datos:
        ultimo = lista_datos[-1]
        
        # Tarjetas de Indicadores (KPIs)
        k1, k2, k3, k4 = st.columns(4)
        with k1:
            st.metric("🌡️ Temperatura", f"{ultimo.get('temperature', 0.0):.1f} °C")
        with k2:
            st.metric("💧 Humedad", f"{ultimo.get('humidity', 0.0):.1f} %")
        with k3:
            rssi = int(ultimo.get("wifi_rssi_dbm", -70))
            st.metric("📶 Señal WiFi RSSI", f"{rssi} dBm", delta="Buena" if rssi > -70 else "Débil")
        with k4:
            st_rele = ultimo.get("relay_active", False)
            st.metric("⚡ Estado de Relé", "ACTIVADO" if st_rele else "APAGADO")

        # Gráfico histórico temporal
        df_hist = pd.DataFrame(lista_datos)
        if "timestamp" in df_hist.columns and "temperature" in df_hist.columns:
            df_plot = df_hist.set_index("timestamp")
            
            c_g1, c_g2 = st.columns(2)
            with c_g1:
                st.write("#### Temperatura y Humedad en Vivo")
                st.line_chart(df_plot[["temperature", "humidity"]])
            with c_g2:
                st.write("#### Intensidad de Señal WiFi (dBm)")
                st.line_chart(df_plot[["wifi_rssi_dbm"]])
    else:
        st.info(
            "⏳ Esperando mensajes en el tópico MQTT. "
            "Asegúrate de iniciar el simulador de ESP32 en otra terminal: "
            "`uv run python 11_iot_mqtt_mosquitto_dashboard/simulator_esp32_mqtt.py`"
        )
        
        # Inyector de muestra rápida si el usuario quiere probar inmediatamente
        if st.button("🧪 Inyectar Mensaje de Muestra Local Ahora"):
            mqtt_mgr.publish(topic_sub, {
                "device_id": device_id,
                "temperature": 25.5,
                "humidity": 58.0,
                "wifi_rssi_dbm": -64,
                "relay_active": True,
                "uptime_seconds": 30,
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            })
            st.rerun()

# -------------------------------------------------------------
# TAB 2: CONTROL DE ACTUADORES (PUBLISH)
# -------------------------------------------------------------
with tab_control:
    st.header("🎛️ Publicar Comandos hacia el ESP32")
    st.write(f"Al pulsar estos botones se enviará un paquete JSON formateado al tópico: `{topic_pub}`")

    c_cmd1, c_cmd2, c_cmd3 = st.columns(3)
    
    with c_cmd1:
        with st.container(border=True):
            st.subheader("💡 Control de Relé")
            st.write("Cambia el estado del actuador físico:")
            col_b1, col_b2 = st.columns(2)
            with col_b1:
                if st.button("🟢 Activar Relé", use_container_width=True):
                    ok = mqtt_mgr.publish(topic_pub, {"action": "SET_RELAY", "value": True})
                    if ok:
                        st.success("Mensaje SET_RELAY:True publicado.")
            with col_b2:
                if st.button("🔴 Apagar Relé", use_container_width=True):
                    ok = mqtt_mgr.publish(topic_pub, {"action": "SET_RELAY", "value": False})
                    if ok:
                        st.info("Mensaje SET_RELAY:False publicado.")

    with c_cmd2:
        with st.container(border=True):
            st.subheader("🏓 Enviar Ping")
            st.write("Comprueba la latencia del microcontrolador:")
            if st.button("Enviar Ping a la ESP32", use_container_width=True):
                ok = mqtt_mgr.publish(topic_pub, {"action": "PING", "time": time.time()})
                if ok:
                    st.success("Ping enviado con éxito.")

    with c_cmd3:
        with st.container(border=True):
            st.subheader("✉️ Publicador JSON Personalizado")
            topico_custom = st.text_input("Tópico destino:", value=topic_pub)
            mensaje_custom = st.text_area("Cuerpo JSON:", value='{"action": "ALERT", "msg": "Test desde Streamlit"}')
            if st.button("Publicar en Broker", use_container_width=True, type="primary"):
                try:
                    payload_dict = json.loads(mensaje_custom)
                    if mqtt_mgr.publish(topico_custom, payload_dict):
                        st.success("Mensaje publicado.")
                except json.JSONDecodeError:
                    st.error("JSON inválido.")

# -------------------------------------------------------------
# TAB 3: TERMINAL DE LOGS
# -------------------------------------------------------------
with tab_terminal:
    st.header("📜 Terminal de Tráfico MQTT")
    st.write("Muestra los eventos de red, paquetes transmitidos (TX) y paquetes recibidos (RX):")
    if mqtt_mgr.logs:
        st.code("\n".join(mqtt_mgr.logs), language="log")
    else:
        st.caption("No hay eventos registrados.")

# -------------------------------------------------------------
# TAB 4: GUÍA DE MOSQUITTO Y HERRAMIENTAS CLI
# -------------------------------------------------------------
with tab_mosquitto_guide:
    st.header("📖 Guía de Eclipse Mosquitto y Comandos de Consola")
    st.markdown("""
    ### 1. ¿Cómo instalar Mosquitto en tu computadora?
    - **Windows:** Puedes descargarlo desde [mosquitto.org/download](https://mosquitto.org/download/) o con `winget install EclipseFoundation.Mosquitto`.
    - **Docker (La opción más rápida):**
      ```bash
      docker run -d --name mosquitto -p 1883:1883 -p 9001:9001 eclipse-mosquitto
      ```
    
    ### 2. Probar con las herramientas de terminal (`mosquitto_sub` y `mosquitto_pub`):
    
    **Suscribirse a la telemetría desde tu consola:**
    ```bash
    mosquitto_sub -h broker.emqx.io -t "iot/sensores/ESP32_MQTT_01/telemetria" -v
    ```
    
    **Enviar una orden a la ESP32 desde la consola:**
    ```bash
    mosquitto_pub -h broker.emqx.io -t "iot/sensores/ESP32_MQTT_01/comandos" -m '{"action":"SET_RELAY","value":true}'
    ```
    """)

st.divider()
st.info("💡 Ejecuta este tutorial con: `uv run streamlit run 11_iot_mqtt_mosquitto_dashboard/app.py`")
