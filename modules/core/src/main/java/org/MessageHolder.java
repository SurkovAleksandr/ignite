package org;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class MessageHolder {
    //Список сообщений, которые передаются между узлами
    public static Queue<String> messageList = new LinkedBlockingQueue<>();
}
