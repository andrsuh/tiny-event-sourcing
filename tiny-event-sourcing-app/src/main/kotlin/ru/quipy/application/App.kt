package ru.quipy.application

import java.util.Properties

class App private constructor(val context: Context) {

    companion object {
        private var app: App? = null
        public fun start() {
            val properties = Properties();
            properties.load(App::class.java.classLoader.getResourceAsStream("application.properties"))
            if (app == null) {
                app = App(Context(properties))
            }
        }
    }

}