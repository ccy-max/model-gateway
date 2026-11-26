// cmd/gateway 公共模型网关独立进程入口。
//
// 环境变量：
//
//	GW_PORT                 监听端口（默认 8080）
//	GW_HOST                 监听地址（默认 0.0.0.0）
//	GW_DATA_PATH            配置文件路径（默认 /app/data/data.json）
//	GW_ADMIN_PASSWORD       首启播种：管理密码
//	GW_API_KEY              首启播种：默认客户端密钥
//	GW_UPSTREAM_QODER_URL   首启播种：现有 Qoder 服务地址（如 http://upstream-host:2323/v1）
//	GW_UPSTREAM_QODER_KEY   首启播种：现有 Qoder 服务密钥
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"qoder2api/gateway"
)

// version 可由构建期 ldflags 注入。
var version = "dev"

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func main() {
	port := env("GW_PORT", "8080")
	host := env("GW_HOST", "0.0.0.0")
	dataPath := env("GW_DATA_PATH", "/app/data/data.json")

	st := gateway.NewStore(dataPath)
	if err := st.Load(); err != nil {
		log.Printf("[gateway] 配置加载失败（按空配置继续）: %v", err)
	}

	srv := gateway.NewServer(st, version)
	srv.Seed(
		env("GW_UPSTREAM_QODER_URL", ""),
		env("GW_UPSTREAM_QODER_KEY", ""),
		env("GW_API_KEY", ""),
		env("GW_ADMIN_PASSWORD", ""),
	)

	go st.RunFlusher(10 * time.Second)

	httpSrv := &http.Server{
		Addr:              host + ":" + port,
		Handler:           srv.Routes(),
		ReadHeaderTimeout: 15 * time.Second,
		IdleTimeout:       30 * time.Second, // 空闲连接较快回收，减少「容器重建后客户端复用死连接」的概率
	}

	go func() {
		log.Printf("[gateway] 公共模型网关启动: http://%s:%s (version=%s, data=%s)", host, port, version, dataPath)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[gateway] 监听失败: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("[gateway] 收到退出信号，优雅关闭...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = httpSrv.Shutdown(ctx)
	st.Close()
}
