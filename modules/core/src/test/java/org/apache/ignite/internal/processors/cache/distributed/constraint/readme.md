###Проблемы с которыми столкнулся
1. При старте теста выполняется метод приводящий к исключению
    org.apache.ignite.testframework.GridTestUtils.resolvePath
    в котором 
    File file = new File(path).getAbsoluteFile();
    всегда(?) возвращает неправильный результат 
    D:\Projects\Ignite\fork\modules\core\modules\core\src\test\config\tests.properties
    из-за того что path равен(его наверно и надо менять)
    modules/core/src/test/config\tests.properties
    Проверить текущий каталог можно так
    new File(".").getAbsoluteFile().
    
    Эту ошибку можно решить выставив -DIGNITE_HOME=D:\Projects\Ignite\fork
    
    