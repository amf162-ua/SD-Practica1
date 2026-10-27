import sys
import subprocess
import os
import tkinter as tk
from tkinter import ttk, messagebox


class WSLauncher:
    """
    Lanzador gráfico para desplegar Estaciones de Riego en el laboratorio (PC LAB 2).
    Cada estación lanza dos procesos en consolas independientes:
      - WM_WS_M (Monitor): se conecta a Central y sirve al Engine
      - WM_WS_E (Engine): se conecta al Monitor y gestiona el riego
    """

    def __init__(self, root):
        self.root = root
        self.root.title("WaterManagement — Launcher de Estaciones (PC LAB 2)")
        self.root.geometry("600x420")
        self.procesos = []
        self._crear_interfaz()

    def _crear_interfaz(self):
        tk.Label(
            self.root,
            text="LANZADOR DE ESTACIONES DE RIEGO (WM_WS_M + WM_WS_E)",
            font=("Helvetica", 10, "bold"),
            bg="#27ae60", fg="white", pady=8
        ).pack(fill=tk.X)

        # ── Conexión a Central y Kafka (Cloud/Railway) ──
        frame_net = ttk.LabelFrame(self.root, text=" Conexión a Central & Kafka (Railway/Cloud) ", padding=10)
        frame_net.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_net, text="IP/Host Central:").grid(row=0, column=0, padx=5, pady=2, sticky=tk.W)
        self.entry_ip_c = ttk.Entry(frame_net, width=20)
        self.entry_ip_c.insert(0, "44.204.33.94")
        self.entry_ip_c.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(frame_net, text="Puerto Central:").grid(row=0, column=2, padx=5, pady=2, sticky=tk.W)
        self.entry_puerto_c = ttk.Entry(frame_net, width=8)
        self.entry_puerto_c.insert(0, "5000")
        self.entry_puerto_c.grid(row=0, column=3, padx=5, pady=2)

        ttk.Label(frame_net, text="IP/Host Kafka:").grid(row=1, column=0, padx=5, pady=2, sticky=tk.W)
        self.entry_ip_k = ttk.Entry(frame_net, width=20)
        self.entry_ip_k.insert(0, "44.204.33.94")
        self.entry_ip_k.grid(row=1, column=1, padx=5, pady=2)

        ttk.Label(frame_net, text="Puerto Kafka:").grid(row=1, column=2, padx=5, pady=2, sticky=tk.W)
        self.entry_puerto_k = ttk.Entry(frame_net, width=8)
        self.entry_puerto_k.insert(0, "9092")
        self.entry_puerto_k.grid(row=1, column=3, padx=5, pady=2)

        # ── Datos de la Estación ──
        frame_ws = ttk.LabelFrame(self.root, text=" Crear Estación de Riego ", padding=10)
        frame_ws.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(frame_ws, text="ID Estación:").grid(row=0, column=0, padx=5, pady=2)
        self.entry_ws_id = ttk.Entry(frame_ws, width=10)
        self.entry_ws_id.insert(0, "WS_01")
        self.entry_ws_id.grid(row=0, column=1, padx=5, pady=2)

        ttk.Label(frame_ws, text="Puerto Local Engine:").grid(row=0, column=2, padx=5, pady=2)
        self.entry_puerto_ws = ttk.Entry(frame_ws, width=8)
        self.entry_puerto_ws.insert(0, "6001")
        self.entry_puerto_ws.grid(row=0, column=3, padx=5, pady=2)

        ttk.Label(frame_ws, text="Ubicación:").grid(row=1, column=0, padx=5, pady=2)
        self.entry_ubicacion = ttk.Entry(frame_ws, width=30)
        self.entry_ubicacion.insert(0, "Parque Central")
        self.entry_ubicacion.grid(row=1, column=1, columnspan=3, padx=5, pady=2, sticky=tk.W)

        ttk.Button(
            frame_ws, text="▶ Lanzar WS (Monitor + Engine)",
            command=self.lanzar_estacion
        ).grid(row=2, column=0, columnspan=4, pady=10)

        # ── Control ──
        frame_ctrl = ttk.Frame(self.root, padding=10)
        frame_ctrl.pack(fill=tk.X, padx=10, pady=5)

        self.lbl_estado = ttk.Label(frame_ctrl, text="Estaciones lanzadas: 0", font=("Helvetica", 9, "bold"))
        self.lbl_estado.pack(side=tk.LEFT)

        ttk.Button(frame_ctrl, text="🛑 Detener todas", command=self.detener).pack(side=tk.RIGHT)

    def lanzar_estacion(self):
        ip_c      = self.entry_ip_c.get().strip()
        puerto_c  = self.entry_puerto_c.get().strip()
        ip_k      = self.entry_ip_k.get().strip()
        puerto_k  = self.entry_puerto_k.get().strip()
        ws_id     = self.entry_ws_id.get().strip()
        puerto_ws = self.entry_puerto_ws.get().strip()
        ubicacion = self.entry_ubicacion.get().strip() or "Parque Central"

        if not ws_id or not puerto_ws:
            messagebox.showwarning("Faltan datos", "Rellena el ID y el puerto de la estación.")
            return

        ws_dir   = os.path.join(os.path.dirname(os.path.abspath(__file__)), "WM_WS")
        script_m = os.path.join(ws_dir, "WM_WS_M.py")
        script_e = os.path.join(ws_dir, "WM_WS_E.py")

        # Lanzar Monitor con ubicación como argumento adicional
        cmd_m = [sys.executable, "-u", script_m,
                 puerto_ws, ip_c, puerto_c, ws_id, ubicacion]
        # Lanzar Engine con ws_id como argumento opcional
        cmd_e = [sys.executable, "-u", script_e,
                 ip_k, puerto_k, "127.0.0.1", puerto_ws, ws_id]

        if sys.platform == "win32":
            p_m = subprocess.Popen(
                ["cmd.exe", "/k"] + cmd_m,
                creationflags=subprocess.CREATE_NEW_CONSOLE, cwd=ws_dir
            )
            p_e = subprocess.Popen(
                ["cmd.exe", "/k"] + cmd_e,
                creationflags=subprocess.CREATE_NEW_CONSOLE, cwd=ws_dir
            )
        else:
            p_m = subprocess.Popen(["x-terminal-emulator", "-e"] + cmd_m, cwd=ws_dir)
            p_e = subprocess.Popen(["x-terminal-emulator", "-e"] + cmd_e, cwd=ws_dir)

        self.procesos.extend([p_m, p_e])

        # Auto-incrementar ID y puerto para la siguiente estación
        self.entry_puerto_ws.delete(0, tk.END)
        self.entry_puerto_ws.insert(0, str(int(puerto_ws) + 1))

        num = "".join(filter(str.isdigit, ws_id))
        if num:
            self.entry_ws_id.delete(0, tk.END)
            self.entry_ws_id.insert(0, f"WS_{int(num)+1:02d}")

        n = len(self.procesos) // 2
        self.lbl_estado.config(text=f"Estaciones lanzadas: {n}")

    def detener(self):
        for p in self.procesos:
            try:
                p.terminate()
            except Exception:
                pass
        self.procesos.clear()
        self.lbl_estado.config(text="Estaciones lanzadas: 0")


if __name__ == "__main__":
    root = tk.Tk()
    app = WSLauncher(root)
    root.mainloop()