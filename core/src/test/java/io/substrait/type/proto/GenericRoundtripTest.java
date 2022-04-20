package io.substrait.type.proto;

import com.google.protobuf.ByteString;
import io.substrait.expression.Expression;
import io.substrait.expression.ExpressionCreator;
import io.substrait.expression.proto.ExpressionProtoConverter;
import io.substrait.expression.proto.ProtoExpressionConverter;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class GenericRoundtripTest {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(GenericRoundtripTest.class);

    @Test
    void genericExpressionTest() {
        //We list all public and static methods of ExpressionCreator
        List<Method> methodsToTest = getMethods(ExpressionCreator.class, true, true);

        // We generate synthetic input params (for a subset of types we support)
        for (Method m : methodsToTest) {
            List<Object> paramInst = instantiateParams(m);

            // If we have all params we perform a roundtrip test.
            if (paramInst.size() == m.getParameterCount()) {
                prettyPrintInvocation(m, paramInst);
                roundtripTest(m, paramInst);
            } else {
                System.out.println("Skipping method: " + m.getName() + " becasue we don't support all params.");
            }

        }

    }

    private void roundtripTest(Method m, List<Object> paramInst) {
        Expression val = null;
        try {
            val = (Expression) m.invoke(null, paramInst.toArray(new Object[0]));
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        var to = new ExpressionProtoConverter(null);
        var from = new ProtoExpressionConverter(null, null, null);
        assertEquals(val, from.from(val.accept(to)));
    }


    private void prettyPrintInvocation(Method m, List<Object> paramInst) {
        List<String> params = new ArrayList<>();
        for (Class p : m.getParameterTypes()) {
            params.add(p.getName());
        }
        System.out.println("Invoking " + m.getName() + " (" + params + ") with values (" + paramInst + ")");
    }

    private List<Object> instantiateParams(Method m) {
        String paramName = "";
        List<Object> l = new ArrayList<>();
        for (Class param : m.getParameterTypes()) {
            Object val = valGenerator(param);
            if (val == null) {
                System.out.println("We can't yet handle type: " + param.getName());
                break;
            }
            l.add(valGenerator(param));
        }
        return l;
    }

    private List<Method> getMethods(Class c, boolean filterPublicOnly, boolean filterStaticOnly) {
        Method[] allMethods = c.getMethods();
        List<Method> selectedMethods = new ArrayList<>();
        for (Method m : allMethods) {
            if ((filterPublicOnly && !Modifier.isPublic(m.getModifiers())) ||
                    (filterStaticOnly && !Modifier.isStatic(m.getModifiers()))) {
                continue;
            }
            selectedMethods.add(m);
        }
        return selectedMethods;
    }


    private Object valGenerator(Class<?> type) {
        Random rand = new Random();

        if (type.equals(Boolean.TYPE)) {
            return rand.nextBoolean();
        } else if (type.equals((Integer.TYPE)) || type.equals(Integer.class)) {
            // we generate always "1" as this is often use for timestamp construction
            // and need to respect the days/months/years formats.
            return 1;
        } else if (type.equals(Long.TYPE) || type.equals(Long.class)) {
            return rand.nextLong();
        } else if (type.equals(Double.TYPE) || type.equals(Double.class)) {
            return rand.nextDouble();
        } else if (type.equals(Float.TYPE) || type.equals(Float.class)) {
            return rand.nextFloat();
        } else if (type.equals(String.class)) {
            return UUID.randomUUID().toString();
        } else if (type.equals(UUID.class)) {
            return UUID.randomUUID();
        } else if (type.equals(LocalDateTime.class)) {
            return LocalDateTime.now();
        } else if (type.equals(ByteString.class)) {
            return ByteString.copyFromUtf8(UUID.randomUUID().toString());
        } else if (type.equals(BigDecimal.class)) {
            return BigDecimal.valueOf(rand.nextLong());
        } else if (type.equals(Instant.class)){
            return Instant.now();
        }

        return null;
    }
}