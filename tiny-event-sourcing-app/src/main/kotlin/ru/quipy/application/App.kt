package ru.quipy.application

import ru.quipy.application.context.Context
import ru.quipy.core.EventSourcingProperties
import java.util.Properties

class App private constructor(val context: Context) {

    companion object {
        private var app: App? = null
        fun start() {
            val properties = Properties();
            properties.load(App::class.java.classLoader.getResourceAsStream("application.properties"))
            if (app == null) {
                app = App(Context(properties, eventSourcingProperties = EventSourcingProperties()))
            }
        }
    }

}