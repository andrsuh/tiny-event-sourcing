package ru.quipy.user.security

import org.springframework.security.core.authority.SimpleGrantedAuthority
import org.springframework.security.core.userdetails.UserDetails
import org.springframework.security.core.userdetails.UserDetailsService
import org.springframework.security.core.userdetails.UsernameNotFoundException
import org.springframework.stereotype.Service
import ru.quipy.user.service.UserRepository
import java.util.*


@Service
class UserDetailsService(
    private val repository: UserRepository
) : UserDetailsService {
    override fun loadUserByUsername(username: String): UserDetails {
        val user = repository.findOneByEmail(username)
                ?: throw UsernameNotFoundException("$username not found")

        return UserSecurity(
            id = user.email,
            email = user.email,
            uPassword = user.password,
            uAuthorities = Collections.singleton(SimpleGrantedAuthority(user.role))
        )
    }
}