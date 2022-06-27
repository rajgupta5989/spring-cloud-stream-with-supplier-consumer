package org.springframework.cloud.stream.function;

import com.example.rabbit.Properties;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.autoconfigure.AutoConfigureAfter;
import org.springframework.boot.autoconfigure.AutoConfigureBefore;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionInspector;
import org.springframework.cloud.function.context.config.ContextFunctionCatalogAutoConfiguration;
import org.springframework.cloud.stream.binding.BindableProxyFactory;
import org.springframework.cloud.stream.config.BindingServiceProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.lang.Nullable;

/**
 * User: rajeshgupta
 * Date: 22/06/22
 */
@Configuration
@AutoConfigureAfter(ContextFunctionCatalogAutoConfiguration.class)
@AutoConfigureBefore(FunctionConfiguration.class)
public class CustomConfig {

   // @Bean
    public InitializingBean functionBindingRegistrar1(Environment environment, FunctionCatalog functionCatalog,
                                                      StreamFunctionProperties streamFunctionProperties,
                                                      Properties properties) {
        return new CustomFunctionBindingRegistrar(functionCatalog, streamFunctionProperties, properties);
    }

   // @Bean
    public InitializingBean functionInitializer1(FunctionCatalog functionCatalog, FunctionInspector functionInspector,
                                                 StreamFunctionProperties functionProperties, @Nullable BindableProxyFactory[] bindableProxyFactories,
                                                 BindingServiceProperties serviceProperties, ConfigurableApplicationContext applicationContext,
                                                 CustomFunctionBindingRegistrar bindingHolder, StreamBridge streamBridge) {
        return new CustomFunctionToDestinationBinder(functionCatalog, functionProperties,
                serviceProperties, streamBridge);
    }
}
