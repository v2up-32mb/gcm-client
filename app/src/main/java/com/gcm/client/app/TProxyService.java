/*
 ============================================================================
 Name        : TProxyService.java
 Author      : hev <r@hev.cc>
 Copyright   : Copyright (c) 2024 xyz
 Description : TProxy Service
 ============================================================================
 */

package com.gcm.client.app;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager.NameNotFoundException;
import android.content.pm.ServiceInfo;
import android.net.ConnectivityManager;
import android.net.Network;
import android.net.VpnService;
import android.os.Build;
import android.os.ParcelFileDescriptor;
import android.util.Log;

import androidx.core.app.NotificationCompat;

import gcm.Gcm;

public class TProxyService extends VpnService {
    private static final String TAG = "TProxyService";
    private static final String CHANNEL_ID = "socks5";
    private static final int NOTIFICATION_ID = 1;

    private static native boolean TProxyStartService(String configPath, int fd);
    private static native boolean TProxyStopService();
    private static native boolean TProxyIsRunning();
    private static native long[] TProxyGetStats();

    public static final String ACTION_CONNECT = "com.gcm.client.app.CONNECT";
    public static final String ACTION_DISCONNECT = "com.gcm.client.app.DISCONNECT";
    public static final String ACTION_STATUS = "com.gcm.client.app.STATUS";
    public static final String EXTRA_STATUS = "status";
    public static final String EXTRA_ERROR = "error";
    public static final String STATUS_STARTING = "starting";
    public static final String STATUS_STARTED = "started";
    public static final String STATUS_STOPPED = "stopped";
    public static final String STATUS_ERROR = "error";

    static {
        System.loadLibrary("hev-socks5-tunnel");
    }

    private volatile ParcelFileDescriptor tunFd;
    private volatile boolean starting;
    private volatile boolean stopping;
    private volatile boolean runtimeRunning;
    private final Object networkLock = new Object();
    private ConnectivityManager connectivityManager;
    private ConnectivityManager.NetworkCallback networkCallback;
    private Network defaultNetwork;
    private boolean networkReconnectPending;

    @Override
    public void onCreate() {
        super.onCreate();
        initNotificationChannel();
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        if (intent != null && ACTION_DISCONNECT.equals(intent.getAction())) {
            new Thread(this::stopService, "gcm-vpn-stop").start();
            return START_NOT_STICKY;
        }

        synchronized (this) {
            if (starting || tunFd != null) {
                return START_STICKY;
            }
            starting = true;
            stopping = false;
        }

        new Preferences(this).setEnable(false);
        startForegroundNotification("正在启动 VPN");
        sendStatus(STATUS_STARTING, null);

        new Thread(() -> {
            try {
                startVpn();
            } finally {
                starting = false;
            }
        }, "gcm-vpn-start").start();
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        if (!stopping) {
            cleanupRuntime();
            new Preferences(this).setEnable(false);
            sendStatus(STATUS_STOPPED, null);
        }
        super.onDestroy();
    }

    @Override
    public void onRevoke() {
        stopService();
        super.onRevoke();
    }

    private void startVpn() {
        Preferences prefs = new Preferences(this);
        try {
            VpnService.Builder builder = buildVpnInterface(prefs);
            tunFd = builder.establish();
            if (tunFd == null) {
                throw new IllegalStateException("系统未能建立 VPN 接口");
            }

            File configFile = writeTProxyConfig(prefs);
            startGcm(prefs);

            if (!TProxyStartService(configFile.getAbsolutePath(), tunFd.getFd())) {
                throw new IllegalStateException("hev-socks5-tunnel 启动失败");
            }
            Thread.sleep(200);
            if (!TProxyIsRunning()) {
                throw new IllegalStateException("hev-socks5-tunnel 未进入运行状态");
            }

            runtimeRunning = true;
            registerNetworkCallback();
            prefs.setEnable(true);
            updateNotification("VPN 已连接");
            sendStatus(STATUS_STARTED, null);
            monitorNativeTunnel(prefs);
        } catch (Throwable error) {
            failStartup(prefs, error);
        }
    }

    private VpnService.Builder buildVpnInterface(Preferences prefs) throws NameNotFoundException {
        String session = "";
        VpnService.Builder builder = new VpnService.Builder();
        builder.setBlocking(false);
        builder.setMtu(prefs.getTunnelMtu());

        if (prefs.getIpv4()) {
            builder.addAddress(prefs.getTunnelIpv4Address(), prefs.getTunnelIpv4Prefix());
            builder.addRoute("0.0.0.0", 0);
            if (!prefs.getRemoteDns() && !prefs.getDnsIpv4().isEmpty()) {
                builder.addDnsServer(prefs.getDnsIpv4());
            }
            session += "IPv4";
        }
        if (prefs.getIpv6() && !prefs.getDisableIpv6Route()) {
            builder.addAddress(prefs.getTunnelIpv6Address(), prefs.getTunnelIpv6Prefix());
            builder.addRoute("::", 0);
            if (!prefs.getRemoteDns() && !prefs.getDnsIpv6().isEmpty()) {
                builder.addDnsServer(prefs.getDnsIpv6());
            }
            if (!session.isEmpty()) {
                session += " + ";
            }
            session += "IPv6";
        }
        if (prefs.getRemoteDns()) {
            builder.addDnsServer(prefs.getMappedDns());
        }

        boolean disallowSelf = true;
        if (prefs.getGlobal()) {
            session += "/Global";
        } else {
            for (String appName : prefs.getApps()) {
                try {
                    builder.addAllowedApplication(appName);
                    disallowSelf = false;
                } catch (NameNotFoundException ignored) {
                }
            }
            session += "/per-App";
        }
        if (disallowSelf) {
            builder.addDisallowedApplication(getApplicationContext().getPackageName());
        }
        builder.setSession(session);
        return builder;
    }

    private File writeTProxyConfig(Preferences prefs) throws IOException {
        File configFile = new File(getCacheDir(), "tproxy.conf");
        StringBuilder config = new StringBuilder()
                .append("misc:\n")
                .append("  task-stack-size: ").append(prefs.getTaskStackSize()).append("\n")
                .append("tunnel:\n")
                .append("  mtu: ").append(prefs.getTunnelMtu()).append("\n")
                .append("socks5:\n")
                .append("  port: ").append(prefs.getSocksPort()).append("\n")
                .append("  address: '").append(prefs.getSocksAddress()).append("'\n")
                .append("  udp: '").append(prefs.getUdpInTcp() ? "tcp" : "udp").append("'\n");

        if (!prefs.getSocksUdpAddress().isEmpty()) {
            config.append("  udp-address: '").append(prefs.getSocksUdpAddress()).append("'\n");
        }
        if (!prefs.getSocksUsername().isEmpty() && !prefs.getSocksPassword().isEmpty()) {
            config.append("  username: '").append(prefs.getSocksUsername()).append("'\n");
            config.append("  password: '").append(prefs.getSocksPassword()).append("'\n");
        }
        if (prefs.getRemoteDns()) {
            config.append("mapdns:\n")
                    .append("  address: ").append(prefs.getMappedDns()).append("\n")
                    .append("  port: 53\n")
                    .append("  network: 240.0.0.0\n")
                    .append("  netmask: 240.0.0.0\n")
                    .append("  cache-size: 10000\n");
        }

        try (FileOutputStream output = new FileOutputStream(configFile, false)) {
            output.write(config.toString().getBytes(StandardCharsets.UTF_8));
        }
        return configFile;
    }

    private void startGcm(Preferences prefs) throws Exception {
        String workerHost = prefs.getWorkerHost().trim();
        if (workerHost.startsWith("wss://")) {
            workerHost = workerHost.substring(6);
        } else if (workerHost.startsWith("https://")) {
            workerHost = workerHost.substring(8);
        }
        workerHost = workerHost.replaceAll("/+$", "");

        Gcm.startSocksProxy(
                prefs.getSocksAddress() + ":" + prefs.getSocksPort(),
                workerHost,
                prefs.getWsConn(),
                prefs.getPrefIp(),
                prefs.getUserID(),
                prefs.getFallbackIp(),
                prefs.getEchDomain(),
                prefs.getEchDns(),
                !prefs.getDisableEch(),
                prefs.getDisableIpv6Route(),
                prefs.getEnableDnsWarmup(),
                prefs.getBypassPrivate(),
                prefs.getBypassGeoIpCn(),
                prefs.getBypassGeoSiteCn(),
                prefs.getBypassRules(),
                true
        );
    }

    private void failStartup(Preferences prefs, Throwable error) {
        String message = error.getMessage();
        if (message == null || message.trim().isEmpty()) {
            message = error.getClass().getSimpleName();
        }
        Log.e(TAG, "VPN startup failed: " + message, error);
        stopping = true;
        cleanupRuntime();
        prefs.setEnable(false);
        sendStatus(STATUS_ERROR, message);
        stopForeground(true);
        stopSelf();
    }

    private void monitorNativeTunnel(Preferences prefs) {
        new Thread(() -> {
            while (!stopping && prefs.getEnable()) {
                try {
                    Thread.sleep(1_000);
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    return;
                }
                if (!stopping && !TProxyIsRunning()) {
                    failStartup(prefs, new IllegalStateException("hev-socks5-tunnel 意外停止"));
                    return;
                }
            }
        }, "gcm-vpn-monitor").start();
    }

    private void stopService() {
        synchronized (this) {
            if (stopping) {
                return;
            }
            stopping = true;
        }
        cleanupRuntime();
        new Preferences(this).setEnable(false);
        sendStatus(STATUS_STOPPED, null);
        stopForeground(true);
        stopSelf();
    }

    private void cleanupRuntime() {
        runtimeRunning = false;
        unregisterNetworkCallback();
        try {
            TProxyStopService();
        } catch (Throwable error) {
            Log.w(TAG, "Failed to stop native tunnel", error);
        }
        try {
            Gcm.stopSocksProxy();
        } catch (Throwable error) {
            Log.w(TAG, "Failed to stop GCM proxy", error);
        }
        ParcelFileDescriptor currentTunFd = tunFd;
        tunFd = null;
        if (currentTunFd != null) {
            try {
                currentTunFd.close();
            } catch (IOException error) {
                Log.w(TAG, "Failed to close TUN fd", error);
            }
        }
    }

    private void registerNetworkCallback() {
        ConnectivityManager manager = (ConnectivityManager) getSystemService(Context.CONNECTIVITY_SERVICE);
        if (manager == null) {
            Log.w(TAG, "ConnectivityManager unavailable; network change recovery is disabled");
            return;
        }

        ConnectivityManager.NetworkCallback callback = new ConnectivityManager.NetworkCallback() {
            @Override
            public void onAvailable(Network network) {
                boolean changed;
                synchronized (networkLock) {
                    changed = defaultNetwork != null && !defaultNetwork.equals(network);
                    defaultNetwork = network;
                }
                if (changed) {
                    scheduleNetworkReconnect();
                }
            }

            @Override
            public void onLost(Network network) {
                boolean lost;
                synchronized (networkLock) {
                    lost = defaultNetwork != null && defaultNetwork.equals(network);
                    if (lost) {
                        defaultNetwork = null;
                    }
                }
                if (lost) {
                    scheduleNetworkReconnect();
                }
            }
        };

        synchronized (networkLock) {
            connectivityManager = manager;
            networkCallback = callback;
            defaultNetwork = null;
        }
        try {
            manager.registerDefaultNetworkCallback(callback);
        } catch (RuntimeException error) {
            synchronized (networkLock) {
                connectivityManager = null;
                networkCallback = null;
                defaultNetwork = null;
            }
            Log.w(TAG, "Failed to register network callback", error);
        }
    }

    private void unregisterNetworkCallback() {
        ConnectivityManager manager;
        ConnectivityManager.NetworkCallback callback;
        synchronized (networkLock) {
            manager = connectivityManager;
            callback = networkCallback;
            connectivityManager = null;
            networkCallback = null;
            defaultNetwork = null;
            networkReconnectPending = false;
        }
        if (manager != null && callback != null) {
            try {
                manager.unregisterNetworkCallback(callback);
            } catch (RuntimeException error) {
                Log.w(TAG, "Failed to unregister network callback", error);
            }
        }
    }

    private void scheduleNetworkReconnect() {
        synchronized (networkLock) {
            if (!runtimeRunning || networkReconnectPending) {
                return;
            }
            networkReconnectPending = true;
        }

        new Thread(() -> {
            try {
                Thread.sleep(300);
                if (runtimeRunning) {
                    Gcm.notifyNetworkChanged();
                }
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
            } catch (Throwable error) {
                Log.w(TAG, "Failed to reconnect after network change", error);
            } finally {
                synchronized (networkLock) {
                    networkReconnectPending = false;
                }
            }
        }, "gcm-network-reconnect").start();
    }

    private void sendStatus(String status, String error) {
        Intent intent = new Intent(ACTION_STATUS);
        intent.setPackage(getPackageName());
        intent.putExtra(EXTRA_STATUS, status);
        if (error != null) {
            intent.putExtra(EXTRA_ERROR, error);
        }
        sendBroadcast(intent);
    }

    private Notification buildNotification(String statusText) {
        Intent intent = new Intent(this, ProfileListActivity.class);
        intent.setFlags(Intent.FLAG_ACTIVITY_CLEAR_TOP | Intent.FLAG_ACTIVITY_SINGLE_TOP);
        PendingIntent pendingIntent = PendingIntent.getActivity(this, 0, intent, PendingIntent.FLAG_IMMUTABLE);
        return new NotificationCompat.Builder(this, CHANNEL_ID)
                .setContentTitle(getString(R.string.app_name))
                .setContentText(statusText)
                .setSmallIcon(android.R.drawable.sym_def_app_icon)
                .setContentIntent(pendingIntent)
                .setOngoing(true)
                .build();
    }

    private void startForegroundNotification(String statusText) {
        Notification notification = buildNotification(statusText);
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.UPSIDE_DOWN_CAKE) {
            startForeground(NOTIFICATION_ID, notification);
        } else {
            startForeground(NOTIFICATION_ID, notification, ServiceInfo.FOREGROUND_SERVICE_TYPE_SPECIAL_USE);
        }
    }

    private void updateNotification(String statusText) {
        NotificationManager manager = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        manager.notify(NOTIFICATION_ID, buildNotification(statusText));
    }

    private void initNotificationChannel() {
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.O) {
            NotificationManager manager = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
            NotificationChannel channel = new NotificationChannel(
                    CHANNEL_ID,
                    getString(R.string.app_name),
                    NotificationManager.IMPORTANCE_LOW
            );
            manager.createNotificationChannel(channel);
        }
    }
}
