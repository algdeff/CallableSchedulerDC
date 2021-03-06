package SchedulerDC.Logic.DistributedComputing;

import java.io.Serializable;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class ProducerTaskUnit implements Callable<Object>, Serializable {

    private List<Integer> _inputValues = new ArrayList<>();

    public ProducerTaskUnit(List<Integer> inputValues) {
        _inputValues.addAll(inputValues);
    }

    @Override
    public Object call() throws Exception {
        return calculate();
    }

    private Object calculate() {
        Integer result = 0;

        for (Integer value : _inputValues) {
            result += value >> 2;

            try {
                TimeUnit.MILLISECONDS.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
//            System.out.println(result);

        }


        //return result;
        return piSpigot(result); //in process
    }

    public String piSpigot(final int n) {
        // найденные цифры сразу же будем записывать в StringBuilder
        StringBuilder pi = new StringBuilder(n);
        int boxes = n * 10 / 3;	// размер массива
        int reminders[] = new int[boxes];
        // инициализируем масив двойками
        for (int i = 0; i < boxes; i++) {
            reminders[i] = 2;
        }
        int heldDigits = 0;    // счётчик временно недействительных цифр
        for (int i = 0; i < n; i++) {
            int carriedOver = 0;    // перенос на следующий шаг
            int sum = 0;
            for (int j = boxes - 1; j >= 0; j--) {
                reminders[j] *= 10;
                sum = reminders[j] + carriedOver;
                int quotient = sum / (j * 2 + 1);   // результат деления суммы на знаменатель
                reminders[j] = sum % (j * 2 + 1);   // остаток от деления суммы на знаменатель
                carriedOver = quotient * j;   // j - числитель
            }
            reminders[0] = sum % 10;
            int q = sum / 10;	// новая цифра числа Пи
            // регулировка недействительных цифр
            if (q == 9) {
                heldDigits++;
            } else if (q == 10) {
                q = 0;
                for (int k = 1; k <= heldDigits; k++) {
                    int replaced = Integer.parseInt(pi.substring(i - k, i - k + 1));
                    if (replaced == 9) {
                        replaced = 0;
                    } else {
                        replaced++;
                    }
                    pi.deleteCharAt(i - k);
                    pi.insert(i - k, replaced);
                }
                heldDigits = 1;
            } else {
                heldDigits = 1;
            }
            pi.append(q);	// сохраняем найденную цифру
        }
        if (pi.length() >= 2) {
            pi.insert(1, '.');	// добавляем в строчку точку после 3
        }
        return pi.toString();
    }


}
