package fins

import (
	"context"
	"log"
	"time"
)

// LoggingInterceptor creates an interceptor that logs all operations
// It logs operation start, end, duration, and any errors
//
// Example:
//
//	client.SetInterceptor(fins.LoggingInterceptor(log.Default()))
//
// Output:
//
//	[FINS] Starting ReadWords - Area:0x82 Address:100
//	[FINS] Completed ReadWords - Duration:5ms
func LoggingInterceptor(logger *log.Logger) Interceptor {
	if logger == nil {
		logger = log.Default()
	}

	return func(ctx context.Context, info *InterceptorInfo, invoker Invoker) (interface{}, error) {
		start := time.Now()

		// Log operation start
		logger.Printf("[FINS] Starting %s - Area:0x%02X Address:%d", info.Operation, info.MemoryArea, info.Address)

		// Execute the operation
		result, err := invoker(ctx)

		// Log operation end with duration
		duration := time.Since(start)
		if err != nil {
			logger.Printf("[FINS] Failed %s - Duration:%v Error:%v", info.Operation, duration, err)
		} else {
			logger.Printf("[FINS] Completed %s - Duration:%v", info.Operation, duration)
		}

		return result, err
	}
}
