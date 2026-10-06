package com.techbleat.bank.config;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.security.config.Customizer;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.http.SessionCreationPolicy;
import org.springframework.security.oauth2.core.DelegatingOAuth2TokenValidator;
import org.springframework.security.oauth2.core.OAuth2TokenValidator;
import org.springframework.security.oauth2.jwt.Jwt;
import org.springframework.security.oauth2.jwt.JwtClaimNames;
import org.springframework.security.oauth2.jwt.JwtClaimValidator;
import org.springframework.security.oauth2.jwt.JwtDecoder;
import org.springframework.security.oauth2.jwt.JwtDecoders;
import org.springframework.security.oauth2.jwt.JwtValidators;
import org.springframework.security.oauth2.jwt.NimbusJwtDecoder;
import org.springframework.security.oauth2.jwt.SupplierJwtDecoder;
import org.springframework.security.web.SecurityFilterChain;

/**
 * Validates the OIDC ID token that ingress-nginx forwards from oauth2-proxy
 * as "Authorization: Bearer ...". Auth is enabled when oidc.issuer is set;
 * without it (local docker-compose) every request is allowed.
 */
@Configuration
public class SecurityConfig {

    private static final Logger log = LoggerFactory.getLogger(SecurityConfig.class);

    @Value("${oidc.issuer:}")
    private String issuer;

    @Value("${oidc.audience:}")
    private String audience;

    @Bean
    public SecurityFilterChain securityFilterChain(HttpSecurity http) throws Exception {
        http.csrf(csrf -> csrf.disable())
                .cors(Customizer.withDefaults())
                .sessionManagement(s -> s.sessionCreationPolicy(SessionCreationPolicy.STATELESS));

        if (issuer.isBlank()) {
            log.warn("OIDC_ISSUER is not set - API authentication is DISABLED");
            http.authorizeHttpRequests(auth -> auth.anyRequest().permitAll());
            return http.build();
        }

        http.authorizeHttpRequests(auth -> auth
                        .requestMatchers("/health", "/actuator/**", "/error").permitAll()
                        .anyRequest().authenticated())
                .oauth2ResourceServer(oauth2 -> oauth2.jwt(jwt -> jwt.decoder(jwtDecoder())));
        return http.build();
    }

    private JwtDecoder jwtDecoder() {
        // Resolve the issuer's JWKS lazily so startup doesn't depend on the IdP being reachable
        return new SupplierJwtDecoder(() -> {
            NimbusJwtDecoder decoder = JwtDecoders.fromIssuerLocation(issuer);
            OAuth2TokenValidator<Jwt> validator = JwtValidators.createDefaultWithIssuer(issuer);
            if (!audience.isBlank()) {
                validator = new DelegatingOAuth2TokenValidator<>(validator,
                        new JwtClaimValidator<List<String>>(JwtClaimNames.AUD,
                                aud -> aud != null && aud.contains(audience)));
            }
            decoder.setJwtValidator(validator);
            return decoder;
        });
    }
}
