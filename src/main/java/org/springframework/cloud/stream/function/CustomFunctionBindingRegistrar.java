package org.springframework.cloud.stream.function;

import com.example.rabbit.Properties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.support.BeanDefinitionRegistry;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.cloud.function.context.FunctionCatalog;
import org.springframework.cloud.function.context.catalog.FunctionTypeUtils;
import org.springframework.cloud.function.context.catalog.SimpleFunctionRegistry;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.EnvironmentAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.util.ObjectUtils;
import org.springframework.util.StringUtils;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * User: rajeshgupta
 * Date: 22/06/22
 */
public class CustomFunctionBindingRegistrar implements InitializingBean, ApplicationContextAware, EnvironmentAware {

    private final static String SOURCE_PROPERTY = "spring.cloud.stream.source";

    protected final Log logger = LogFactory.getLog(getClass());

    private final FunctionCatalog functionCatalog;

    private final StreamFunctionProperties streamFunctionProperties;

    private ConfigurableApplicationContext applicationContext;

    private Environment environment;

    private int inputCount;

    private int outputCount;

    private Properties properties;

    CustomFunctionBindingRegistrar(FunctionCatalog functionCatalog, StreamFunctionProperties streamFunctionProperties, Properties properties) {
        this.functionCatalog = functionCatalog;
        this.streamFunctionProperties = streamFunctionProperties;
        this.properties = properties;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (ObjectUtils.isEmpty(applicationContext.getBeanNamesForAnnotation(EnableBinding.class))) {
            BeanDefinitionRegistry registry = (BeanDefinitionRegistry) applicationContext.getBeanFactory();

            if (StringUtils.hasText(streamFunctionProperties.getDefinition())) {
                String[] functionDefinitions = this.filterEligibleFunctionDefinitions();
                for (String functionDefinition : functionDefinitions) {
                    RootBeanDefinition functionBindableProxyDefinition = new RootBeanDefinition(CustomBindableFunctionProxyFactory.class);
                    SimpleFunctionRegistry.FunctionInvocationWrapper function = functionCatalog.lookup(functionDefinition);

                    if (function != null) {
                        Type functionType = function.getFunctionType();
                        if (function.isSupplier()) {
                            this.inputCount = 0;
                            this.outputCount = this.getOutputCount(functionType, true);
                        }
                        else if (function.isConsumer()) {
                            this.inputCount = FunctionTypeUtils.getInputCount(functionType);
                            this.outputCount = 0;
                        }
                        else {
                            this.inputCount = FunctionTypeUtils.getInputCount(functionType);
                            this.outputCount = this.getOutputCount(functionType, false);
                        }

                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(functionDefinition);
                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(this.inputCount);
                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(this.outputCount);
                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(this.streamFunctionProperties);
                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(this.properties);
                        try {
                            String name = functionDefinition + "_binding";
                            registry.registerBeanDefinition(name, functionBindableProxyDefinition);
                        }
                        catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    else {
                        logger.warn("The function definition '" + streamFunctionProperties.getDefinition() +
                                "' is not valid. The referenced function bean or one of its components does not exist");
                    }
                }
            }

            if (StringUtils.hasText(this.environment.getProperty(SOURCE_PROPERTY))) {
                String[] sourceNames = this.environment.getProperty(SOURCE_PROPERTY).split(";");

                for (String sourceName : sourceNames) {
                    if (functionCatalog.lookup(sourceName) == null) {
                        RootBeanDefinition functionBindableProxyDefinition = new RootBeanDefinition(CustomBindableFunctionProxyFactory.class);
                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(sourceName);
                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(0);
                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(1);
                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(this.streamFunctionProperties);
                        functionBindableProxyDefinition.getConstructorArgumentValues().addGenericArgumentValue(this.properties);
                        registry.registerBeanDefinition(sourceName + "_binding1", functionBindableProxyDefinition);
                    }
                }
            }
        }
    }

    private int getOutputCount(Type functionType, boolean isSupplier) {
        int outputCount = FunctionTypeUtils.getOutputCount(functionType);
        if (!isSupplier && functionType instanceof ParameterizedType) {
            Type outputType = ((ParameterizedType) functionType).getActualTypeArguments()[1];
            if (FunctionTypeUtils.isMono(outputType) && outputType instanceof ParameterizedType
                    && FunctionTypeUtils.getRawType(((ParameterizedType) outputType).getActualTypeArguments()[0]).equals(Void.class)) {
                outputCount = 0;
            }
            else if (FunctionTypeUtils.getRawType(outputType).equals(Void.class)) {
                outputCount = 0;
            }
        }
        return outputCount;
    }

    private String[] filterEligibleFunctionDefinitions() {
        List<String> eligibleFunctionDefinitions = new ArrayList<>();
        String[] functionDefinitions = streamFunctionProperties.getDefinition().split(";");
        for (String functionDefinition : functionDefinitions) {
            String[] functionNames = StringUtils.delimitedListToStringArray(functionDefinition.replaceAll(",", "|").trim(), "|");
            boolean eligibleDefinition = true;
            for (int i = 0; i < functionNames.length && eligibleDefinition; i++) {
                String functionName = functionNames[i];
                if (this.applicationContext.containsBean(functionName)) {
                    Object functionBean = this.applicationContext.getBean(functionName);
                    Type functionType = FunctionTypeUtils.discoverFunctionType(functionBean, functionName, (GenericApplicationContext) this.applicationContext);
                    String functionTypeStringValue = functionType.toString();
                    if (functionTypeStringValue.contains("KTable") || functionTypeStringValue.contains("KStream")) {
                        eligibleDefinition = false;
                    }
                }
                else {
                    logger.warn("You have defined function definition that does not exist: " + functionName);
                }
            }
            if (eligibleDefinition) {
                eligibleFunctionDefinitions.add(functionDefinition);
            }
        }
        return eligibleFunctionDefinitions.toArray(new String[0]);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = (ConfigurableApplicationContext) applicationContext;
    }

    @Override
    public void setEnvironment(Environment environment) {
        this.environment = environment;
    }
}
