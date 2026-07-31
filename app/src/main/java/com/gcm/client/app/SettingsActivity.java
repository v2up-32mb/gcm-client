/*
 ============================================================================
 Name        : SettingsActivity.java
 Author      : Claude Code
 Description : Global Settings Activity
 ============================================================================
 */

package com.gcm.client.app;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.widget.Button;
import android.widget.CheckBox;
import android.widget.EditText;
import android.widget.Toast;

public class SettingsActivity extends Activity {
    private Preferences prefs;

    private CheckBox checkbox_global;
    private Button button_apps;
    private EditText edittext_socks_port;
    private Button btn_save;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        prefs = new Preferences(this);
        setContentView(R.layout.activity_settings);

        // 设置标题
        setTitle("全局设置");

        // 初始化控件
        checkbox_global = findViewById(R.id.checkbox_global);
        button_apps = findViewById(R.id.button_apps);
        edittext_socks_port = findViewById(R.id.edittext_socks_port);
        btn_save = findViewById(R.id.btn_save);

        // 加载当前设置
        loadSettings();

        // 设置选择应用按钮点击事件（始终可用）
        button_apps.setOnClickListener(v -> {
            startActivity(new Intent(this, AppListActivity.class));
        });

        // 设置保存按钮点击事件
        btn_save.setOnClickListener(v -> {
            if (saveSettings()) {
                Toast.makeText(this, "设置已保存", Toast.LENGTH_SHORT).show();
                finish();
            }
        });
    }

    private void loadSettings() {
        // 加载全局设置
        checkbox_global.setChecked(prefs.getGlobal());
        edittext_socks_port.setText(String.valueOf(prefs.getSocksPort()));

        // 检查 VPN 是否正在运行
        boolean isVpnRunning = prefs.getEnable();
        if (isVpnRunning) {
            // VPN 运行时禁用所有全局设置的修改
            checkbox_global.setEnabled(false);
            button_apps.setEnabled(false);
            edittext_socks_port.setEnabled(false);
            btn_save.setEnabled(false);

            Toast.makeText(this, "VPN 正在运行，无法修改全局设置", Toast.LENGTH_LONG).show();
        }
    }

    @Override
    public void onBackPressed() {
        saveSettings();
        super.onBackPressed();
    }

    private boolean saveSettings() {
        // 验证并保存端口
        int port = 1080;
        try {
            port = Integer.parseInt(edittext_socks_port.getText().toString().trim());
        } catch (Exception e) {
            Toast.makeText(this, "端口号格式错误", Toast.LENGTH_SHORT).show();
            return false;
        }
        if (port < 1024) {
            Toast.makeText(this, "端口号必须 ≥ 1024", Toast.LENGTH_SHORT).show();
            return false;
        }

        // 保存全局设置
        prefs.setGlobal(checkbox_global.isChecked());
        prefs.setSocksPort(port);

        return true;
    }
}
