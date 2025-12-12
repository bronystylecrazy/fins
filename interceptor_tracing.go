package fins

import (
	"context"
	"log"
)

// TracingInterceptor creates an interceptor that extracts and logs trace IDs from context
// The trace ID is extracted from the context using the provided key.
//
// Example:
//
//	client.SetInterceptor(fins.TracingInterceptor("traceID"))
//
//	// Use with context
//	ctx := context.WithValue(context.Background(), "traceID", "trace-12345")
//	client.ReadWords(ctx, fins.MemoryAreaDMWord, 100, 5)
//	// Output: [TRACE:trace-12345] ReadWords - Address:100
func TracingInterceptor(traceIDKey interface{}) Interceptor {
	return func(ctx context.Context, info *InterceptorInfo, invoker Invoker) (interface{}, error) {
		// Get trace ID from context
		traceID := ctx.Value(traceIDKey)
		if traceID != nil {
			log.Printf("[TRACE:%v] %s - Address:%d", traceID, info.Operation, info.Address)
		}

		return invoker(ctx)
	}
}

// TracingInterceptorWithLogger creates a tracing interceptor with a custom logger
func TracingInterceptorWithLogger(traceIDKey interface{}, logger *log.Logger) Interceptor {
	if logger == nil {
		logger = log.Default()
	}

	return func(ctx context.Context, info *InterceptorInfo, invoker Invoker) (interface{}, error) {
		// Get trace ID from context
		traceID := ctx.Value(traceIDKey)
		if traceID != nil {
			logger.Printf("[TRACE:%v] %s - Area:0x%02X Address:%d",
				traceID, info.Operation, info.MemoryArea, info.Address)
		}

		return invoker(ctx)
	}
}
